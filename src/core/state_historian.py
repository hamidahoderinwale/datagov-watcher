"""
Concordance: Dataset State Historian
Builds versioned timeline of dataset internal states over time
"""

import os
import json
import hashlib
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import logging
import requests
import pandas as pd
from pathlib import Path
import shutil

logger = logging.getLogger(__name__)

class DatasetStateHistorian:
    def __init__(self, base_dir: str = "dataset_states", db_path: str = "datasets.db"):
        self.base_dir = Path(base_dir)
        self.db_path = db_path
        self.base_dir.mkdir(exist_ok=True)
        self.init_state_tables()
    
    def init_state_tables(self):
        """Initialize database tables for state tracking"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Dataset state snapshots
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dataset_states (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id TEXT,
                snapshot_date DATE,
                state_hash TEXT,
                file_path TEXT,
                metadata_path TEXT,
                schema_hash TEXT,
                content_hash TEXT,
                row_count INTEGER,
                column_count INTEGER,
                file_size INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # State diffs
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS state_diffs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id TEXT,
                from_date DATE,
                to_date DATE,
                diff_type TEXT,
                diff_data TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Schema changes
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS schema_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id TEXT,
                change_date DATE,
                change_type TEXT,
                column_name TEXT,
                old_type TEXT,
                new_type TEXT,
                old_nullable TEXT,
                new_nullable TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Metadata changes
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS metadata_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id TEXT,
                change_date DATE,
                field_name TEXT,
                old_value TEXT,
                new_value TEXT,
                change_type TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def create_dataset_timeline(self, dataset_id: str) -> Path:
        """Create timeline directory for a dataset"""
        timeline_dir = self.base_dir / dataset_id
        timeline_dir.mkdir(exist_ok=True)
        return timeline_dir
    
    def snapshot_dataset_state(self, dataset: Dict, force_refresh: bool = False) -> Dict:
        """Create a snapshot of dataset state"""
        dataset_id = dataset.get('id', '')
        snapshot_date = datetime.now().strftime('%Y-%m-%d')
        
        # Check if snapshot already exists for today
        if not force_refresh and self._snapshot_exists(dataset_id, snapshot_date):
            logger.info(f"Snapshot already exists for {dataset_id} on {snapshot_date}")
            return self._get_snapshot(dataset_id, snapshot_date)
        
        logger.info(f"Creating snapshot for dataset {dataset_id}")
        
        # Create timeline directory
        timeline_dir = self.create_dataset_timeline(dataset_id)
        snapshot_dir = timeline_dir / snapshot_date
        snapshot_dir.mkdir(exist_ok=True)
        
        # Download and analyze dataset
        state_info = self._analyze_dataset_state(dataset, snapshot_dir)
        
        # Store in database
        self._store_snapshot(dataset_id, snapshot_date, state_info, snapshot_dir)
        
        # Compare with previous snapshot
        self._compare_with_previous(dataset_id, snapshot_date, state_info)
        
        return state_info
    
    def _analyze_dataset_state(self, dataset: Dict, snapshot_dir: Path) -> Dict:
        """Analyze dataset state and extract structured information"""
        dataset_id = dataset.get('id', '')
        url = dataset.get('url', '')
        
        state_info = {
            'dataset_id': dataset_id,
            'title': dataset.get('title', ''),
            'agency': dataset.get('agency', ''),
            'url': url,
            'snapshot_date': datetime.now().strftime('%Y-%m-%d'),
            'metadata': dataset,
            'files': [],
            'schema': {},
            'content_stats': {},
            'availability': 'unknown'
        }
        
        # Check URL availability
        try:
            response = requests.head(url, timeout=10, allow_redirects=True)
            state_info['availability'] = 'available' if response.status_code == 200 else 'unavailable'
            state_info['status_code'] = response.status_code
        except Exception as e:
            state_info['availability'] = 'error'
            state_info['error'] = str(e)
        
        # Try to download and analyze data files
        if state_info['availability'] == 'available':
            try:
                # Download main data file
                data_file = self._download_data_file(url, snapshot_dir)
                if data_file:
                    state_info['files'].append(data_file)
                    
                    # Analyze schema and content
                    schema_info = self._analyze_schema(data_file['path'])
                    state_info['schema'] = schema_info
                    
                    content_stats = self._analyze_content(data_file['path'])
                    state_info['content_stats'] = content_stats
                    
            except Exception as e:
                logger.warning(f"Failed to analyze data for {dataset_id}: {e}")
                state_info['analysis_error'] = str(e)
        
        # Save metadata
        metadata_file = snapshot_dir / 'metadata.json'
        with open(metadata_file, 'w') as f:
            json.dump(state_info, f, indent=2, default=str)
        
        return state_info
    
    def _download_data_file(self, url: str, snapshot_dir: Path) -> Optional[Dict]:
        """Download data file and return file info"""
        try:
            response = requests.get(url, timeout=30, stream=True)
            response.raise_for_status()
            
            # Determine file extension
            content_type = response.headers.get('content-type', '')
            if 'csv' in content_type:
                ext = '.csv'
            elif 'json' in content_type:
                ext = '.json'
            elif 'xlsx' in content_type:
                ext = '.xlsx'
            else:
                ext = '.data'
            
            # Save file
            filename = f"data{ext}"
            file_path = snapshot_dir / filename
            
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            # Calculate hash
            file_hash = self._calculate_file_hash(file_path)
            
            return {
                'filename': filename,
                'path': str(file_path),
                'size': file_path.stat().st_size,
                'hash': file_hash,
                'content_type': content_type,
                'url': url
            }
            
        except Exception as e:
            logger.warning(f"Failed to download {url}: {e}")
            return None
    
    def _analyze_schema(self, file_path: str) -> Dict:
        """Analyze file schema"""
        try:
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path, nrows=1000)  # Sample for schema
                schema = {
                    'columns': list(df.columns),
                    'dtypes': df.dtypes.to_dict(),
                    'column_count': len(df.columns),
                    'sample_rows': df.head(5).to_dict('records')
                }
            elif file_path.endswith('.json'):
                with open(file_path, 'r') as f:
                    data = json.load(f)
                schema = {
                    'type': 'json',
                    'keys': list(data.keys()) if isinstance(data, dict) else 'array',
                    'structure': self._analyze_json_structure(data)
                }
            else:
                schema = {'type': 'unknown', 'file_extension': Path(file_path).suffix}
            
            return schema
            
        except Exception as e:
            logger.warning(f"Failed to analyze schema for {file_path}: {e}")
            return {'error': str(e)}
    
    def _analyze_content(self, file_path: str) -> Dict:
        """Analyze content statistics"""
        try:
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path)
                stats = {
                    'row_count': len(df),
                    'column_count': len(df.columns),
                    'memory_usage': df.memory_usage(deep=True).sum(),
                    'null_counts': df.isnull().sum().to_dict(),
                    'dtypes': df.dtypes.to_dict()
                }
            elif file_path.endswith('.json'):
                with open(file_path, 'r') as f:
                    data = json.load(f)
                stats = {
                    'type': 'json',
                    'size': len(str(data)),
                    'structure': self._analyze_json_structure(data)
                }
            else:
                stats = {'type': 'unknown'}
            
            return stats
            
        except Exception as e:
            logger.warning(f"Failed to analyze content for {file_path}: {e}")
            return {'error': str(e)}
    
    def _analyze_json_structure(self, data: Any, max_depth: int = 3) -> Dict:
        """Analyze JSON structure recursively"""
        if isinstance(data, dict):
            return {
                'type': 'object',
                'keys': list(data.keys()),
                'size': len(data),
                'children': {k: self._analyze_json_structure(v, max_depth-1) 
                           for k, v in list(data.items())[:5]}  # Sample first 5 keys
            }
        elif isinstance(data, list):
            return {
                'type': 'array',
                'length': len(data),
                'sample_item': self._analyze_json_structure(data[0], max_depth-1) if data else None
            }
        else:
            return {
                'type': type(data).__name__,
                'value': str(data)[:100]  # Truncate long values
            }
    
    def _calculate_file_hash(self, file_path: Path) -> str:
        """Calculate SHA256 hash of file"""
        hash_sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_sha256.update(chunk)
        return hash_sha256.hexdigest()
    
    def _snapshot_exists(self, dataset_id: str, snapshot_date: str) -> bool:
        """Check if snapshot exists for given date"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT COUNT(*) FROM dataset_states 
            WHERE dataset_id = ? AND snapshot_date = ?
        ''', (dataset_id, snapshot_date))
        
        count = cursor.fetchone()[0]
        conn.close()
        
        return count > 0
    
    def _get_snapshot(self, dataset_id: str, snapshot_date: str) -> Dict:
        """Get existing snapshot"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM dataset_states 
            WHERE dataset_id = ? AND snapshot_date = ?
            ORDER BY created_at DESC LIMIT 1
        ''', (dataset_id, snapshot_date))
        
        result = cursor.fetchone()
        conn.close()
        
        if result:
            # Load metadata file
            timeline_dir = self.base_dir / dataset_id / snapshot_date
            metadata_file = timeline_dir / 'metadata.json'
            if metadata_file.exists():
                with open(metadata_file, 'r') as f:
                    return json.load(f)
        
        return {}
    
    def _store_snapshot(self, dataset_id: str, snapshot_date: str, state_info: Dict, snapshot_dir: Path):
        """Store snapshot in database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Calculate hashes
        state_hash = hashlib.sha256(json.dumps(state_info, sort_keys=True).encode()).hexdigest()
        schema_hash = hashlib.sha256(json.dumps(state_info.get('schema', {}), sort_keys=True).encode()).hexdigest()
        content_hash = hashlib.sha256(json.dumps(state_info.get('content_stats', {}), sort_keys=True).encode()).hexdigest()
        
        # Get file info
        file_path = str(snapshot_dir / 'metadata.json')
        file_size = snapshot_dir.stat().st_size if snapshot_dir.exists() else 0
        
        # Get counts
        row_count = state_info.get('content_stats', {}).get('row_count', 0)
        column_count = state_info.get('schema', {}).get('column_count', 0)
        
        cursor.execute('''
            INSERT INTO dataset_states 
            (dataset_id, snapshot_date, state_hash, file_path, metadata_path, 
             schema_hash, content_hash, row_count, column_count, file_size)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (dataset_id, snapshot_date, state_hash, file_path, file_path,
              schema_hash, content_hash, row_count, column_count, file_size))
        
        conn.commit()
        conn.close()
    
    def _compare_with_previous(self, dataset_id: str, snapshot_date: str, current_state: Dict):
        """Compare current state with previous snapshot"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get previous snapshot
        cursor.execute('''
            SELECT * FROM dataset_states 
            WHERE dataset_id = ? AND snapshot_date < ?
            ORDER BY snapshot_date DESC LIMIT 1
        ''', (dataset_id, snapshot_date))
        
        prev_result = cursor.fetchone()
        
        if prev_result:
            prev_id, prev_dataset_id, prev_date, prev_state_hash, prev_file_path, prev_metadata_path, prev_schema_hash, prev_content_hash, prev_row_count, prev_column_count, prev_file_size, prev_created_at = prev_result
            
            # Load previous state
            prev_timeline_dir = self.base_dir / dataset_id / prev_date
            prev_metadata_file = prev_timeline_dir / 'metadata.json'
            
            if prev_metadata_file.exists():
                with open(prev_metadata_file, 'r') as f:
                    prev_state = json.load(f)
                
                # Compare states
                self._generate_state_diff(dataset_id, prev_date, snapshot_date, prev_state, current_state)
        
        conn.close()
    
    def _generate_state_diff(self, dataset_id: str, from_date: str, to_date: str, prev_state: Dict, current_state: Dict):
        """Generate detailed state diff"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Schema changes
        prev_schema = prev_state.get('schema', {})
        curr_schema = current_state.get('schema', {})
        
        if prev_schema and curr_schema:
            schema_diff = self._compare_schemas(prev_schema, curr_schema)
            if schema_diff:
                cursor.execute('''
                    INSERT INTO state_diffs 
                    (dataset_id, from_date, to_date, diff_type, diff_data)
                    VALUES (?, ?, ?, 'schema', ?)
                ''', (dataset_id, from_date, to_date, json.dumps(schema_diff)))
        
        # Metadata changes
        metadata_diff = self._compare_metadata(prev_state.get('metadata', {}), current_state.get('metadata', {}))
        if metadata_diff:
            cursor.execute('''
                INSERT INTO state_diffs 
                (dataset_id, from_date, to_date, diff_type, diff_data)
                VALUES (?, ?, ?, 'metadata', ?)
            ''', (dataset_id, from_date, to_date, json.dumps(metadata_diff)))
        
        # Content changes
        content_diff = self._compare_content(prev_state.get('content_stats', {}), current_state.get('content_stats', {}))
        if content_diff:
            cursor.execute('''
                INSERT INTO state_diffs 
                (dataset_id, from_date, to_date, diff_type, diff_data)
                VALUES (?, ?, ?, 'content', ?)
            ''', (dataset_id, from_date, to_date, json.dumps(content_diff)))
        
        conn.commit()
        conn.close()
    
    def _compare_schemas(self, prev_schema: Dict, curr_schema: Dict) -> Dict:
        """Compare schema changes"""
        changes = {
            'column_changes': [],
            'type_changes': [],
            'row_count_changes': []
        }
        
        # Column changes
        prev_cols = set(prev_schema.get('columns', []))
        curr_cols = set(curr_schema.get('columns', []))
        
        added_cols = curr_cols - prev_cols
        removed_cols = prev_cols - curr_cols
        
        for col in added_cols:
            changes['column_changes'].append(f"+column: {col}")
        for col in removed_cols:
            changes['column_changes'].append(f"-column: {col}")
        
        # Type changes
        prev_dtypes = prev_schema.get('dtypes', {})
        curr_dtypes = curr_schema.get('dtypes', {})
        
        for col in prev_cols & curr_cols:
            if prev_dtypes.get(col) != curr_dtypes.get(col):
                changes['type_changes'].append(f"{col}: {prev_dtypes.get(col)} → {curr_dtypes.get(col)}")
        
        # Row count changes
        prev_rows = prev_schema.get('row_count', 0)
        curr_rows = curr_schema.get('row_count', 0)
        if prev_rows != curr_rows:
            delta = curr_rows - prev_rows
            changes['row_count_changes'].append(f"rows: {prev_rows} → {curr_rows} ({delta:+d})")
        
        return changes
    
    def _compare_metadata(self, prev_metadata: Dict, curr_metadata: Dict) -> Dict:
        """Compare metadata changes"""
        changes = []
        
        # Key fields to compare
        key_fields = ['title', 'description', 'license', 'publisher', 'modified', 'url']
        
        for field in key_fields:
            prev_val = prev_metadata.get(field, '')
            curr_val = curr_metadata.get(field, '')
            
            if prev_val != curr_val:
                changes.append(f"{field}: {prev_val} → {curr_val}")
        
        return {'changes': changes}
    
    def _compare_content(self, prev_content: Dict, curr_content: Dict) -> Dict:
        """Compare content statistics"""
        changes = []
        
        # Row count changes
        prev_rows = prev_content.get('row_count', 0)
        curr_rows = curr_content.get('row_count', 0)
        if prev_rows != curr_rows:
            delta = curr_rows - prev_rows
            pct = (delta / prev_rows * 100) if prev_rows > 0 else 0
            changes.append(f"row_count: {prev_rows} → {curr_rows} ({delta:+d}, {pct:+.1f}%)")
        
        # Column count changes
        prev_cols = prev_content.get('column_count', 0)
        curr_cols = curr_content.get('column_count', 0)
        if prev_cols != curr_cols:
            changes.append(f"column_count: {prev_cols} → {curr_cols}")
        
        return {'changes': changes}
    
    def get_dataset_timeline(self, dataset_id: str) -> List[Dict]:
        """Get timeline of all snapshots for a dataset"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT snapshot_date, row_count, column_count, file_size, created_at
            FROM dataset_states 
            WHERE dataset_id = ?
            ORDER BY snapshot_date ASC
        ''', (dataset_id,))
        
        columns = [description[0] for description in cursor.description]
        timeline = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        conn.close()
        return timeline
    
    def get_state_diffs(self, dataset_id: str) -> List[Dict]:
        """Get all state diffs for a dataset"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT from_date, to_date, diff_type, diff_data, created_at
            FROM state_diffs 
            WHERE dataset_id = ?
            ORDER BY from_date ASC
        ''', (dataset_id,))
        
        columns = [description[0] for description in cursor.description]
        diffs = []
        
        for row in cursor.fetchall():
            diff = dict(zip(columns, row))
            diff['diff_data'] = json.loads(diff['diff_data'])
            diffs.append(diff)
        
        conn.close()
        return diffs
    
    def generate_volatility_score(self, dataset_id: str) -> Dict:
        """Calculate volatility score for a dataset"""
        timeline = self.get_dataset_timeline(dataset_id)
        diffs = self.get_state_diffs(dataset_id)
        
        if not timeline:
            return {'volatility_score': 0, 'change_count': 0, 'age_days': 0}
        
        # Calculate age
        first_date = datetime.strptime(timeline[0]['snapshot_date'], '%Y-%m-%d')
        last_date = datetime.strptime(timeline[-1]['snapshot_date'], '%Y-%m-%d')
        age_days = (last_date - first_date).days
        
        # Count changes
        change_count = len(diffs)
        
        # Calculate volatility score
        volatility_score = change_count / (age_days / 365.25) if age_days > 0 else 0
        
        return {
            'volatility_score': volatility_score,
            'change_count': change_count,
            'age_days': age_days,
            'snapshots': len(timeline)
        }
