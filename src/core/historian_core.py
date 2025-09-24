"""
Dataset State Historian - Core Components
Implements the comprehensive dataset state tracking system with LIL integration
"""

import sqlite3
import json
import hashlib
import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
import logging
import pandas as pd
import numpy as np
from dataclasses import dataclass
import requests
import time
import zipfile
import io
import xml.etree.ElementTree as ET

logger = logging.getLogger(__name__)

@dataclass
class DatasetSnapshot:
    """Represents a complete snapshot of a dataset at a point in time"""
    dataset_id: str
    snapshot_date: str
    title: str
    agency: str
    publisher: str
    license: str
    landing_page: str
    modified: str
    resources: List[Dict]
    schema: Dict
    fingerprint: Dict
    metadata: Dict
    file_path: Optional[str] = None
    manifest_path: Optional[str] = None

@dataclass
class DatasetDiff:
    """Represents differences between two dataset snapshots"""
    dataset_id: str
    from_date: str
    to_date: str
    metadata_changes: List[Dict]
    schema_changes: List[Dict]
    content_changes: Dict
    volatility_score: float
    change_events: List[Dict]

class DatasetStateHistorian:
    """Core class for managing dataset state history"""
    
    def __init__(self, db_path: str = "datasets.db", data_dir: str = "dataset_states"):
        self.db_path = db_path
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        
        # Create subdirectories
        (self.data_dir / "snapshots").mkdir(exist_ok=True)
        (self.data_dir / "diffs").mkdir(exist_ok=True)
        (self.data_dir / "lil_data").mkdir(exist_ok=True)
        (self.data_dir / "wayback_data").mkdir(exist_ok=True)
        
        self.init_database()
    
    def init_database(self):
        """Initialize database tables for the historian"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Enhanced dataset snapshots table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS historian_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id TEXT NOT NULL,
                snapshot_date TEXT NOT NULL,
                source TEXT NOT NULL,  -- 'lil', 'live', 'wayback', 'eota'
                title TEXT,
                agency TEXT,
                publisher TEXT,
                license TEXT,
                landing_page TEXT,
                modified TEXT,
                resources TEXT,  -- JSON
                schema_data TEXT,  -- JSON
                fingerprint TEXT,  -- JSON
                metadata TEXT,  -- JSON
                file_path TEXT,
                manifest_path TEXT,
                provenance TEXT,  -- JSON - archival source info
                status TEXT DEFAULT 'active',  -- 'active', 'vanished', 'archived'
                last_seen_date TEXT,  -- For vanished datasets
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(dataset_id, snapshot_date, source)
            )
        ''')
        
        # Dataset diffs table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS historian_diffs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id TEXT NOT NULL,
                from_date TEXT NOT NULL,
                to_date TEXT NOT NULL,
                from_source TEXT NOT NULL,
                to_source TEXT NOT NULL,
                metadata_changes TEXT,  -- JSON
                schema_changes TEXT,  -- JSON
                content_changes TEXT,  -- JSON
                volatility_score REAL,
                change_events TEXT,  -- JSON
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(dataset_id, from_date, to_date)
            )
        ''')
        
        # Volatility metrics table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS volatility_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id TEXT NOT NULL,
                metric_name TEXT NOT NULL,
                metric_value REAL,
                snapshot_date TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # LIL catalog index
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS lil_catalog_index (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_date TEXT NOT NULL,
                catalog_url TEXT,
                warc_url TEXT,
                index_url TEXT,
                total_datasets INTEGER,
                processed BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(snapshot_date)
            )
        ''')
        
        # Vanished datasets tracking
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS vanished_datasets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id TEXT NOT NULL UNIQUE,
                last_seen_date TEXT NOT NULL,
                last_seen_source TEXT NOT NULL,
                disappearance_date TEXT,
                last_known_title TEXT,
                last_known_agency TEXT,
                last_known_landing_page TEXT,
                archival_sources TEXT,  -- JSON array of available sources
                status TEXT DEFAULT 'vanished',  -- 'vanished', 'recovered'
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Wayback CDX index cache
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS wayback_cdx_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                original_url TEXT,
                mimetype TEXT,
                status_code INTEGER,
                digest TEXT,
                length INTEGER,
                wayback_url TEXT,
                archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(url, timestamp)
            )
        ''')
        
        # EOTA CDX index cache
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS eota_cdx_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                original_url TEXT,
                mimetype TEXT,
                status_code INTEGER,
                digest TEXT,
                length INTEGER,
                eota_url TEXT,
                archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(url, timestamp)
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def store_snapshot(self, snapshot: DatasetSnapshot) -> bool:
        """Store a dataset snapshot"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO historian_snapshots 
                (dataset_id, snapshot_date, source, title, agency, publisher, 
                 license, landing_page, modified, resources, schema_data, 
                 fingerprint, metadata, file_path, manifest_path)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                snapshot.dataset_id,
                snapshot.snapshot_date,
                'live',  # Default source
                snapshot.title,
                snapshot.agency,
                snapshot.publisher,
                snapshot.license,
                snapshot.landing_page,
                snapshot.modified,
                json.dumps(snapshot.resources),
                json.dumps(snapshot.schema),
                json.dumps(snapshot.fingerprint),
                json.dumps(snapshot.metadata),
                snapshot.file_path,
                snapshot.manifest_path
            ))
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            logger.error(f"Error storing snapshot for {snapshot.dataset_id}: {e}")
            return False
    
    def get_snapshots(self, dataset_id: str, limit: Optional[int] = None) -> List[DatasetSnapshot]:
        """Get all snapshots for a dataset"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        query = '''
            SELECT dataset_id, snapshot_date, source, title, agency, publisher,
                   license, landing_page, modified, resources, schema_data,
                   fingerprint, metadata, file_path, manifest_path
            FROM historian_snapshots
            WHERE dataset_id = ?
            ORDER BY snapshot_date ASC
        '''
        
        if limit:
            query += f" LIMIT {limit}"
        
        cursor.execute(query, (dataset_id,))
        rows = cursor.fetchall()
        conn.close()
        
        snapshots = []
        for row in rows:
            snapshot = DatasetSnapshot(
                dataset_id=row[0],
                snapshot_date=row[1],
                title=row[3],
                agency=row[4],
                publisher=row[5],
                license=row[6],
                landing_page=row[7],
                modified=row[8],
                resources=json.loads(row[9]) if row[9] else [],
                schema=json.loads(row[10]) if row[10] else {},
                fingerprint=json.loads(row[11]) if row[11] else {},
                metadata=json.loads(row[12]) if row[12] else {},
                file_path=row[13],
                manifest_path=row[14]
            )
            snapshots.append(snapshot)
        
        return snapshots
    
    def compute_diff(self, from_snapshot: DatasetSnapshot, to_snapshot: DatasetSnapshot) -> DatasetDiff:
        """Compute differences between two snapshots"""
        metadata_changes = self._compute_metadata_diff(from_snapshot, to_snapshot)
        schema_changes = self._compute_schema_diff(from_snapshot, to_snapshot)
        content_changes = self._compute_content_diff(from_snapshot, to_snapshot)
        
        # Calculate volatility score
        volatility_score = self._calculate_volatility_score(
            metadata_changes, schema_changes, content_changes
        )
        
        # Generate change events
        change_events = self._generate_change_events(
            metadata_changes, schema_changes, content_changes
        )
        
        return DatasetDiff(
            dataset_id=from_snapshot.dataset_id,
            from_date=from_snapshot.snapshot_date,
            to_date=to_snapshot.snapshot_date,
            metadata_changes=metadata_changes,
            schema_changes=schema_changes,
            content_changes=content_changes,
            volatility_score=volatility_score,
            change_events=change_events
        )
    
    def _compute_metadata_diff(self, from_snap: DatasetSnapshot, to_snap: DatasetSnapshot) -> List[Dict]:
        """Compute metadata differences"""
        changes = []
        
        # Compare key metadata fields
        metadata_fields = ['title', 'agency', 'publisher', 'license', 'landing_page', 'modified']
        
        for field in metadata_fields:
            from_val = getattr(from_snap, field, '')
            to_val = getattr(to_snap, field, '')
            
            if from_val != to_val:
                changes.append({
                    'field': field,
                    'old_value': from_val,
                    'new_value': to_val,
                    'change_type': 'metadata_update'
                })
        
        return changes
    
    def _compute_schema_diff(self, from_snap: DatasetSnapshot, to_snap: DatasetSnapshot) -> List[Dict]:
        """Compute schema differences"""
        changes = []
        
        from_schema = from_snap.schema
        to_schema = to_snap.schema
        
        # Compare columns
        from_cols = set(from_schema.get('columns', []))
        to_cols = set(to_schema.get('columns', []))
        
        # Added columns
        for col in to_cols - from_cols:
            changes.append({
                'field': 'column',
                'column_name': col,
                'change_type': 'column_added',
                'old_value': None,
                'new_value': col
            })
        
        # Removed columns
        for col in from_cols - to_cols:
            changes.append({
                'field': 'column',
                'column_name': col,
                'change_type': 'column_removed',
                'old_value': col,
                'new_value': None
            })
        
        # Compare row counts
        from_rows = from_schema.get('row_count', 0)
        to_rows = to_schema.get('row_count', 0)
        
        if from_rows != to_rows:
            changes.append({
                'field': 'row_count',
                'change_type': 'row_count_change',
                'old_value': from_rows,
                'new_value': to_rows,
                'delta': to_rows - from_rows
            })
        
        return changes
    
    def _compute_content_diff(self, from_snap: DatasetSnapshot, to_snap: DatasetSnapshot) -> Dict:
        """Compute content differences using fingerprints"""
        from_fp = from_snap.fingerprint
        to_fp = to_snap.fingerprint
        
        # Calculate similarity using minhash if available
        similarity = 1.0
        if 'minhash' in from_fp and 'minhash' in to_fp:
            similarity = self._calculate_minhash_similarity(
                from_fp['minhash'], to_fp['minhash']
            )
        
        return {
            'similarity': similarity,
            'content_drift': 1.0 - similarity,
            'from_fingerprint': from_fp,
            'to_fingerprint': to_fp
        }
    
    def _calculate_minhash_similarity(self, from_hash: List[int], to_hash: List[int]) -> float:
        """Calculate Jaccard similarity between minhashes"""
        if not from_hash or not to_hash:
            return 0.0
        
        from_set = set(from_hash)
        to_set = set(to_hash)
        
        intersection = len(from_set.intersection(to_set))
        union = len(from_set.union(to_set))
        
        return intersection / union if union > 0 else 0.0
    
    def _calculate_volatility_score(self, metadata_changes: List, schema_changes: List, content_changes: Dict) -> float:
        """Calculate overall volatility score for a dataset"""
        score = 0.0
        
        # Metadata changes weight
        score += len(metadata_changes) * 0.3
        
        # Schema changes weight (higher impact)
        score += len(schema_changes) * 0.5
        
        # Content drift weight
        content_drift = content_changes.get('content_drift', 0.0)
        score += content_drift * 0.2
        
        return min(score, 1.0)  # Cap at 1.0
    
    def _generate_change_events(self, metadata_changes: List, schema_changes: List, content_changes: Dict) -> List[Dict]:
        """Generate high-level change events"""
        events = []
        
        # License changes
        license_changes = [c for c in metadata_changes if c['field'] == 'license']
        if license_changes:
            events.append({
                'type': 'license_change',
                'severity': 'high',
                'description': f"License changed from {license_changes[0]['old_value']} to {license_changes[0]['new_value']}"
            })
        
        # Schema changes
        if schema_changes:
            events.append({
                'type': 'schema_change',
                'severity': 'medium',
                'description': f"{len(schema_changes)} schema changes detected"
            })
        
        # Content drift
        content_drift = content_changes.get('content_drift', 0.0)
        if content_drift > 0.15:  # 15% threshold
            events.append({
                'type': 'content_drift',
                'severity': 'high' if content_drift > 0.5 else 'medium',
                'description': f"Significant content drift detected: {content_drift:.1%}"
            })
        
        return events
    
    def store_diff(self, diff: DatasetDiff) -> bool:
        """Store a dataset diff"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO historian_diffs
                (dataset_id, from_date, to_date, from_source, to_source,
                 metadata_changes, schema_changes, content_changes,
                 volatility_score, change_events)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                diff.dataset_id,
                diff.from_date,
                diff.to_date,
                'live',  # Default sources
                'live',
                json.dumps(diff.metadata_changes),
                json.dumps(diff.schema_changes),
                json.dumps(diff.content_changes),
                diff.volatility_score,
                json.dumps(diff.change_events)
            ))
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            logger.error(f"Error storing diff for {diff.dataset_id}: {e}")
            return False
    
    def get_volatility_ranking(self, limit: int = 50) -> List[Dict]:
        """Get datasets ranked by volatility score"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT dataset_id, AVG(volatility_score) as avg_volatility,
                   COUNT(*) as change_count, MAX(created_at) as last_change
            FROM historian_diffs
            GROUP BY dataset_id
            ORDER BY avg_volatility DESC
            LIMIT ?
        ''', (limit,))
        
        results = []
        for row in cursor.fetchall():
            results.append({
                'dataset_id': row[0],
                'volatility_score': row[1],
                'change_count': row[2],
                'last_change': row[3]
            })
        
        conn.close()
        return results
    
    def get_dataset_timeline(self, dataset_id: str) -> Dict:
        """Get comprehensive timeline for a dataset including vanished/archived states"""
        snapshots = self.get_snapshots(dataset_id)
        
        if not snapshots:
            return {'dataset_id': dataset_id, 'timeline': [], 'volatility': 0.0, 'status': 'unknown'}
        
        timeline = []
        dataset_status = 'active'
        
        for i, snapshot in enumerate(snapshots):
            # Determine source and status
            source = getattr(snapshot, 'source', 'live')
            status = getattr(snapshot, 'status', 'active')
            
            timeline_entry = {
                'date': snapshot.snapshot_date,
                'source': source,
                'title': snapshot.title,
                'agency': snapshot.agency,
                'volatility': 0.0,
                'changes': [],
                'status': status,
                'provenance': getattr(snapshot, 'provenance', {})
            }
            
            # Add change information if not the first snapshot
            if i > 0:
                prev_snapshot = snapshots[i-1]
                diff = self.compute_diff(prev_snapshot, snapshot)
                timeline_entry['volatility'] = diff.volatility_score
                timeline_entry['changes'] = diff.change_events
                
                # Add special events for vanished datasets
                if status == 'archived' and prev_snapshot.status == 'active':
                    timeline_entry['changes'].append({
                        'field': 'status',
                        'old_value': 'active',
                        'new_value': 'archived',
                        'change_type': 'dataset_vanished',
                        'description': 'Dataset disappeared from live catalog'
                    })
            
            timeline.append(timeline_entry)
            
            # Update overall status
            if status in ['vanished', 'archived']:
                dataset_status = status
        
        # Calculate overall volatility
        overall_volatility = sum(entry['volatility'] for entry in timeline) / len(timeline) if timeline else 0.0
        
        return {
            'dataset_id': dataset_id,
            'timeline': timeline,
            'volatility': overall_volatility,
            'snapshot_count': len(snapshots),
            'status': dataset_status
        }
    
    def get_vanished_datasets(self) -> List[Dict]:
        """Get list of all vanished datasets"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT vd.dataset_id, vd.last_seen_date, vd.last_seen_source, 
                       vd.disappearance_date, vd.last_known_title, vd.last_known_agency,
                       vd.archival_sources, vd.status,
                       COUNT(hs.id) as archived_snapshots
                FROM vanished_datasets vd
                LEFT JOIN historian_snapshots hs ON vd.dataset_id = hs.dataset_id 
                    AND hs.source IN ('wayback', 'eota', 'lil')
                GROUP BY vd.dataset_id
                ORDER BY vd.disappearance_date DESC
            ''')
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    'dataset_id': row[0],
                    'last_seen_date': row[1],
                    'last_seen_source': row[2],
                    'disappearance_date': row[3],
                    'last_known_title': row[4],
                    'last_known_agency': row[5],
                    'archival_sources': json.loads(row[6]) if row[6] else [],
                    'status': row[7],
                    'archived_snapshots': row[8]
                })
            
            conn.close()
            return results
            
        except Exception as e:
            logger.error(f"Error getting vanished datasets: {e}")
            return []
    
    def get_vanished_dataset_timeline(self, dataset_id: str) -> Dict:
        """Get timeline for a vanished dataset with archived states"""
        try:
            # Check if dataset is vanished
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT * FROM vanished_datasets WHERE dataset_id = ?
            ''', (dataset_id,))
            
            vanished_info = cursor.fetchone()
            if not vanished_info:
                return {'error': 'Dataset not found in vanished datasets'}
            
            # Get all snapshots including archived ones
            cursor.execute('''
                SELECT * FROM historian_snapshots 
                WHERE dataset_id = ? 
                ORDER BY snapshot_date ASC
            ''', (dataset_id,))
            
            snapshots_data = cursor.fetchall()
            conn.close()
            
            if not snapshots_data:
                return {'error': 'No snapshots found for vanished dataset'}
            
            # Convert to DatasetSnapshot objects
            snapshots = []
            for row in snapshots_data:
                snapshot = DatasetSnapshot(
                    dataset_id=row[1],
                    snapshot_date=row[2],
                    title=row[4],
                    agency=row[5],
                    publisher=row[6],
                    license=row[7],
                    landing_page=row[8],
                    modified=row[9],
                    resources=json.loads(row[10]) if row[10] else [],
                    schema=json.loads(row[11]) if row[11] else {},
                    fingerprint=json.loads(row[12]) if row[12] else {},
                    metadata=json.loads(row[13]) if row[13] else {},
                    file_path=row[15],
                    manifest_path=row[16]
                )
                
                # Add additional attributes
                snapshot.source = row[3]
                snapshot.status = row[17] if len(row) > 17 else 'active'
                snapshot.provenance = json.loads(row[16]) if len(row) > 16 and row[16] else {}
                
                snapshots.append(snapshot)
            
            # Generate timeline
            timeline = []
            for i, snapshot in enumerate(snapshots):
                timeline_entry = {
                    'date': snapshot.snapshot_date,
                    'source': snapshot.source,
                    'title': snapshot.title,
                    'agency': snapshot.agency,
                    'volatility': 0.0,
                    'changes': [],
                    'status': snapshot.status,
                    'provenance': snapshot.provenance
                }
                
                # Add change information if not the first snapshot
                if i > 0:
                    prev_snapshot = snapshots[i-1]
                    diff = self.compute_diff(prev_snapshot, snapshot)
                    timeline_entry['volatility'] = diff.volatility_score
                    timeline_entry['changes'] = diff.change_events
                
                timeline.append(timeline_entry)
            
            # Calculate overall volatility
            overall_volatility = sum(entry['volatility'] for entry in timeline) / len(timeline) if timeline else 0.0
            
            return {
                'dataset_id': dataset_id,
                'timeline': timeline,
                'volatility': overall_volatility,
                'snapshot_count': len(snapshots),
                'status': 'vanished',
                'vanished_info': {
                    'last_seen_date': vanished_info[2],
                    'last_seen_source': vanished_info[3],
                    'disappearance_date': vanished_info[4],
                    'archival_sources': json.loads(vanished_info[7]) if vanished_info[7] else []
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting vanished dataset timeline for {dataset_id}: {e}")
            return {'error': str(e)}
    
    def detect_disappearance_events(self) -> List[Dict]:
        """Detect datasets that have disappeared and create disappearance events"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Find datasets that were active but are now vanished
            cursor.execute('''
                SELECT DISTINCT hs1.dataset_id, hs1.snapshot_date as last_active_date,
                       hs1.title, hs1.agency, hs1.landing_page
                FROM historian_snapshots hs1
                WHERE hs1.source = 'live' 
                AND hs1.dataset_id NOT IN (
                    SELECT DISTINCT dataset_id FROM historian_snapshots 
                    WHERE source = 'live' 
                    AND snapshot_date = (
                        SELECT MAX(snapshot_date) FROM historian_snapshots WHERE source = 'live'
                    )
                )
                AND hs1.snapshot_date = (
                    SELECT MAX(snapshot_date) FROM historian_snapshots hs2 
                    WHERE hs2.dataset_id = hs1.dataset_id AND hs2.source = 'live'
                )
            ''')
            
            disappeared = []
            for row in cursor.fetchall():
                disappeared.append({
                    'dataset_id': row[0],
                    'last_active_date': row[1],
                    'title': row[2],
                    'agency': row[3],
                    'landing_page': row[4],
                    'event_type': 'dataset_disappeared',
                    'description': f"Dataset '{row[2]}' disappeared from live catalog after {row[1]}"
                })
            
            conn.close()
            return disappeared
            
        except Exception as e:
            logger.error(f"Error detecting disappearance events: {e}")
            return []


