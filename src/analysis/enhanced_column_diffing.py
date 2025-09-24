#!/usr/bin/env python3
"""
Enhanced Column Diffing System
Implements patch-based diffing with column-level changes and data previews
"""

import json
import sqlite3
import pandas as pd
import hashlib
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
import logging
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

class ChangeType(Enum):
    COLUMN_ADDED = "column_added"
    COLUMN_REMOVED = "column_removed"
    COLUMN_RENAMED = "column_renamed"
    COLUMN_TYPE_CHANGED = "column_type_changed"
    DATA_CHANGED = "data_changed"
    ROW_ADDED = "row_added"
    ROW_REMOVED = "row_removed"

@dataclass
class ColumnChange:
    change_type: ChangeType
    column_name: str
    old_value: Any
    new_value: Any
    old_type: str
    new_type: str
    sample_old_data: List[Any]
    sample_new_data: List[Any]
    change_magnitude: float
    confidence_score: float

@dataclass
class SchemaSnapshot:
    dataset_id: str
    snapshot_date: str
    columns: List[str]
    column_types: Dict[str, str]
    row_count: int
    sample_data: List[Dict[str, Any]]
    schema_hash: str
    content_hash: str

class EnhancedColumnDiffing:
    def __init__(self, db_path: str = "datasets.db"):
        self.db_path = db_path
        self.init_diffing_tables()
    
    def init_diffing_tables(self):
        """Initialize tables for enhanced diffing"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Schema snapshots table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS schema_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id TEXT NOT NULL,
                snapshot_date TEXT NOT NULL,
                columns TEXT NOT NULL,
                column_types TEXT NOT NULL,
                row_count INTEGER NOT NULL,
                sample_data TEXT NOT NULL,
                schema_hash TEXT NOT NULL,
                content_hash TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(dataset_id, snapshot_date)
            )
        ''')
        
        # Column changes table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS column_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id TEXT NOT NULL,
                from_snapshot_date TEXT NOT NULL,
                to_snapshot_date TEXT NOT NULL,
                change_type TEXT NOT NULL,
                column_name TEXT NOT NULL,
                old_value TEXT,
                new_value TEXT,
                old_type TEXT,
                new_type TEXT,
                sample_old_data TEXT,
                sample_new_data TEXT,
                change_magnitude REAL,
                confidence_score REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def create_schema_snapshot(self, dataset_id: str, snapshot_date: str, 
                              data: Dict[str, Any]) -> SchemaSnapshot:
        """Create a schema snapshot from dataset data"""
        
        # Extract schema information
        columns = data.get('columns', [])
        column_types = data.get('column_types', {})
        row_count = data.get('row_count', 0)
        sample_data = data.get('sample_data', [])
        
        # Calculate hashes
        schema_data = {
            'columns': columns,
            'column_types': column_types,
            'row_count': row_count
        }
        schema_hash = hashlib.sha256(json.dumps(schema_data, sort_keys=True).encode()).hexdigest()
        content_hash = data.get('content_hash', '') or hashlib.sha256(str(data).encode()).hexdigest()
        
        return SchemaSnapshot(
            dataset_id=dataset_id,
            snapshot_date=snapshot_date,
            columns=columns,
            column_types=column_types,
            row_count=row_count,
            sample_data=sample_data,
            schema_hash=schema_hash,
            content_hash=content_hash
        )
    
    def store_schema_snapshot(self, snapshot: SchemaSnapshot):
        """Store schema snapshot in database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO schema_snapshots
                (dataset_id, snapshot_date, columns, column_types, row_count, 
                 sample_data, schema_hash, content_hash)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                snapshot.dataset_id,
                snapshot.snapshot_date,
                json.dumps(snapshot.columns),
                json.dumps(snapshot.column_types),
                snapshot.row_count,
                json.dumps(snapshot.sample_data),
                snapshot.schema_hash,
                snapshot.content_hash
            ))
            
            conn.commit()
            
        except Exception as e:
            logger.error(f"Error storing schema snapshot: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def compare_schemas(self, from_snapshot: SchemaSnapshot, 
                       to_snapshot: SchemaSnapshot) -> List[ColumnChange]:
        """Compare two schema snapshots and return column changes"""
        changes = []
        
        from_cols = set(from_snapshot.columns)
        to_cols = set(to_snapshot.columns)
        
        # Find added columns
        for col in to_cols - from_cols:
            changes.append(ColumnChange(
                change_type=ChangeType.COLUMN_ADDED,
                column_name=col,
                old_value=None,
                new_value=to_snapshot.column_types.get(col, 'unknown'),
                old_type='none',
                new_type=to_snapshot.column_types.get(col, 'unknown'),
                sample_old_data=[],
                sample_new_data=self._extract_column_sample(to_snapshot.sample_data, col),
                change_magnitude=1.0,
                confidence_score=1.0
            ))
        
        # Find removed columns
        for col in from_cols - to_cols:
            changes.append(ColumnChange(
                change_type=ChangeType.COLUMN_REMOVED,
                column_name=col,
                old_value=from_snapshot.column_types.get(col, 'unknown'),
                new_value=None,
                old_type=from_snapshot.column_types.get(col, 'unknown'),
                new_type='none',
                sample_old_data=self._extract_column_sample(from_snapshot.sample_data, col),
                sample_new_data=[],
                change_magnitude=1.0,
                confidence_score=1.0
            ))
        
        # Find changed columns (type changes)
        for col in from_cols & to_cols:
            old_type = from_snapshot.column_types.get(col, 'unknown')
            new_type = to_snapshot.column_types.get(col, 'unknown')
            
            if old_type != new_type:
                changes.append(ColumnChange(
                    change_type=ChangeType.COLUMN_TYPE_CHANGED,
                    column_name=col,
                    old_value=old_type,
                    new_value=new_type,
                    old_type=old_type,
                    new_type=new_type,
                    sample_old_data=self._extract_column_sample(from_snapshot.sample_data, col),
                    sample_new_data=self._extract_column_sample(to_snapshot.sample_data, col),
                    change_magnitude=self._calculate_type_change_magnitude(old_type, new_type),
                    confidence_score=0.9
                ))
            
            # Check for data changes in existing columns
            data_change = self._detect_data_changes(
                from_snapshot.sample_data, 
                to_snapshot.sample_data, 
                col
            )
            
            if data_change['changed']:
                changes.append(ColumnChange(
                    change_type=ChangeType.DATA_CHANGED,
                    column_name=col,
                    old_value=data_change['old_sample'],
                    new_value=data_change['new_sample'],
                    old_type=old_type,
                    new_type=new_type,
                    sample_old_data=data_change['old_sample'],
                    sample_new_data=data_change['new_sample'],
                    change_magnitude=data_change['magnitude'],
                    confidence_score=data_change['confidence']
                ))
        
        return changes
    
    def _extract_column_sample(self, sample_data: List[Dict[str, Any]], column: str) -> List[Any]:
        """Extract sample data for a specific column"""
        return [row.get(column) for row in sample_data if column in row][:5]
    
    def _calculate_type_change_magnitude(self, old_type: str, new_type: str) -> float:
        """Calculate magnitude of type change"""
        type_hierarchy = {
            'int': 1, 'float': 2, 'str': 3, 'bool': 4, 
            'datetime': 5, 'object': 6, 'unknown': 7
        }
        
        old_level = type_hierarchy.get(old_type.lower(), 7)
        new_level = type_hierarchy.get(new_type.lower(), 7)
        
        return abs(old_level - new_level) / 7.0
    
    def _detect_data_changes(self, old_data: List[Dict[str, Any]], 
                           new_data: List[Dict[str, Any]], 
                           column: str) -> Dict[str, Any]:
        """Detect data changes in a specific column"""
        old_values = self._extract_column_sample(old_data, column)
        new_values = self._extract_column_sample(new_data, column)
        
        if not old_values and not new_values:
            return {'changed': False, 'magnitude': 0.0, 'confidence': 0.0}
        
        # Simple change detection based on sample data
        old_set = set(str(v) for v in old_values if v is not None)
        new_set = set(str(v) for v in new_values if v is not None)
        
        if old_set == new_set:
            return {'changed': False, 'magnitude': 0.0, 'confidence': 0.0}
        
        # Calculate change magnitude
        intersection = old_set & new_set
        union = old_set | new_set
        jaccard_similarity = len(intersection) / len(union) if union else 0
        magnitude = 1.0 - jaccard_similarity
        
        return {
            'changed': True,
            'magnitude': magnitude,
            'confidence': 0.8,
            'old_sample': old_values,
            'new_sample': new_values
        }
    
    def store_column_changes(self, changes: List[ColumnChange], dataset_id: str, 
                           from_date: str, to_date: str):
        """Store column changes in database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            for change in changes:
                cursor.execute('''
                    INSERT INTO column_changes
                    (dataset_id, from_snapshot_date, to_snapshot_date, change_type,
                     column_name, old_value, new_value, old_type, new_type,
                     sample_old_data, sample_new_data, change_magnitude, confidence_score)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    dataset_id, from_date, to_date, change.change_type.value,
                    change.column_name, json.dumps(change.old_value), json.dumps(change.new_value),
                    change.old_type, change.new_type,
                    json.dumps(change.sample_old_data), json.dumps(change.sample_new_data),
                    change.change_magnitude, change.confidence_score
                ))
            
            conn.commit()
            
        except Exception as e:
            logger.error(f"Error storing column changes: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def get_column_changes(self, dataset_id: str, limit: int = 50) -> List[Dict]:
        """Get column changes for a dataset"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT change_type, column_name, old_value, new_value, old_type, new_type,
                   sample_old_data, sample_new_data, change_magnitude, confidence_score,
                   from_snapshot_date, to_snapshot_date
            FROM column_changes
            WHERE dataset_id = ?
            ORDER BY created_at DESC
            LIMIT ?
        ''', (dataset_id, limit))
        
        changes = []
        for row in cursor.fetchall():
            changes.append({
                'change_type': row[0],
                'column_name': row[1],
                'old_value': json.loads(row[2]) if row[2] else None,
                'new_value': json.loads(row[3]) if row[3] else None,
                'old_type': row[4],
                'new_type': row[5],
                'sample_old_data': json.loads(row[6]) if row[6] else [],
                'sample_new_data': json.loads(row[7]) if row[7] else [],
                'change_magnitude': row[8],
                'confidence_score': row[9],
                'from_date': row[10],
                'to_date': row[11]
            })
        
        conn.close()
        return changes
    
    def process_dataset_snapshots(self, dataset_id: str):
        """Process all snapshots for a dataset to create schema snapshots and detect changes"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get all snapshots for this dataset
        cursor.execute('''
            SELECT snapshot_date, schema, content_hash, row_count, column_count
            FROM dataset_states
            WHERE dataset_id = ?
            ORDER BY snapshot_date ASC
        ''', (dataset_id,))
        
        snapshots = []
        for row in cursor.fetchall():
            schema_data = json.loads(row[1]) if row[1] else {}
            snapshots.append({
                'date': row[0],
                'schema': schema_data,
                'content_hash': row[2],
                'row_count': row[3] or 0,
                'column_count': row[4] or 0
            })
        
        conn.close()
        
        if len(snapshots) < 2:
            return
        
        # Create schema snapshots and compare
        previous_snapshot = None
        
        for snapshot_data in snapshots:
            # Create schema snapshot
            schema_snapshot = self.create_schema_snapshot(
                dataset_id, 
                snapshot_data['date'],
                {
                    'columns': snapshot_data['schema'].get('columns', []),
                    'column_types': snapshot_data['schema'].get('column_types', {}),
                    'row_count': snapshot_data['row_count'],
                    'sample_data': snapshot_data['schema'].get('sample_data', []),
                    'content_hash': snapshot_data['content_hash']
                }
            )
            
            self.store_schema_snapshot(schema_snapshot)
            
            # Compare with previous snapshot
            if previous_snapshot:
                changes = self.compare_schemas(previous_snapshot, schema_snapshot)
                if changes:
                    self.store_column_changes(
                        changes, 
                        dataset_id, 
                        previous_snapshot.snapshot_date, 
                        schema_snapshot.snapshot_date
                    )
                    print(f"Found {len(changes)} column changes for {dataset_id} between {previous_snapshot.snapshot_date} and {schema_snapshot.snapshot_date}")
            
            previous_snapshot = schema_snapshot

def main():
    """Main function to process dataset snapshots"""
    diffing = EnhancedColumnDiffing()
    
    # Get all datasets
    conn = sqlite3.connect("datasets.db")
    cursor = conn.cursor()
    
    cursor.execute('SELECT DISTINCT dataset_id FROM dataset_states LIMIT 10')
    dataset_ids = [row[0] for row in cursor.fetchall()]
    
    conn.close()
    
    print(f"Processing {len(dataset_ids)} datasets for column diffing...")
    
    for dataset_id in dataset_ids:
        print(f"Processing {dataset_id}...")
        diffing.process_dataset_snapshots(dataset_id)
    
    print("Column diffing complete!")

if __name__ == "__main__":
    main()
