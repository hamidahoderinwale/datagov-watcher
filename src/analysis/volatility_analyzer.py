"""
Volatility Analyzer for Dataset State Historian

This module computes volatility metrics including:
- Churn rates (how often datasets change)
- Content drift (similarity changes over time)
- License flip counts (frequency of license changes)
- Schema volatility (structural changes)
- Availability volatility (uptime/downtime patterns)
"""

import sqlite3
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
import hashlib

logger = logging.getLogger(__name__)

@dataclass
class VolatilityMetrics:
    """Container for volatility metrics"""
    dataset_id: str
    churn_rate: float
    content_drift: float
    license_flips: int
    schema_volatility: float
    availability_volatility: float
    last_updated: str
    total_changes: int
    avg_change_frequency: float

class VolatilityAnalyzer:
    """Analyzes dataset volatility and change patterns"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        
    def compute_dataset_volatility(self, dataset_id: str) -> Optional[VolatilityMetrics]:
        """Compute comprehensive volatility metrics for a dataset"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get all snapshots for this dataset
            cursor.execute('''
                SELECT snapshot_date, title, agency, url, description, last_modified,
                       availability, status_code, row_count, column_count, file_size,
                       content_hash, schema, license, publisher
                FROM dataset_states
                WHERE dataset_id = ?
                ORDER BY snapshot_date ASC
            ''', (dataset_id,))
            
            snapshots = cursor.fetchall()
            if len(snapshots) < 2:
                conn.close()
                return None
            
            # Compute metrics
            churn_rate = self._compute_churn_rate(snapshots)
            content_drift = self._compute_content_drift(snapshots)
            license_flips = self._compute_license_flips(snapshots)
            schema_volatility = self._compute_schema_volatility(snapshots)
            availability_volatility = self._compute_availability_volatility(snapshots)
            
            # Calculate change frequency
            total_changes = len(snapshots) - 1
            date_range = (datetime.strptime(snapshots[-1][0], '%Y-%m-%d') - 
                         datetime.strptime(snapshots[0][0], '%Y-%m-%d')).days
            avg_change_frequency = total_changes / max(date_range, 1) if date_range > 0 else 0
            
            conn.close()
            
            return VolatilityMetrics(
                dataset_id=dataset_id,
                churn_rate=churn_rate,
                content_drift=content_drift,
                license_flips=license_flips,
                schema_volatility=schema_volatility,
                availability_volatility=availability_volatility,
                last_updated=snapshots[-1][0],
                total_changes=total_changes,
                avg_change_frequency=avg_change_frequency
            )
            
        except Exception as e:
            logger.error(f"Error computing volatility for {dataset_id}: {e}")
            return None
    
    def _compute_churn_rate(self, snapshots: List[Tuple]) -> float:
        """Compute churn rate - percentage of snapshots that show changes"""
        if len(snapshots) < 2:
            return 0.0
        
        changes = 0
        for i in range(1, len(snapshots)):
            prev = snapshots[i-1]
            curr = snapshots[i]
            
            # Check for changes in key fields
            if (prev[1] != curr[1] or  # title
                prev[2] != curr[2] or  # agency
                prev[3] != curr[3] or  # url
                prev[4] != curr[4] or  # description
                prev[5] != curr[5] or  # last_modified
                prev[6] != curr[6] or  # availability
                prev[7] != curr[7] or  # status_code
                prev[8] != curr[8] or  # row_count
                prev[9] != curr[9] or  # column_count
                prev[10] != curr[10] or  # file_size
                prev[11] != curr[11]):  # content_hash
                changes += 1
        
        return changes / (len(snapshots) - 1) if len(snapshots) > 1 else 0.0
    
    def _compute_content_drift(self, snapshots: List[Tuple]) -> float:
        """Compute content drift - average similarity between consecutive snapshots"""
        if len(snapshots) < 2:
            return 0.0
        
        similarities = []
        for i in range(1, len(snapshots)):
            prev_hash = snapshots[i-1][11]  # content_hash
            curr_hash = snapshots[i][11]
            
            if prev_hash and curr_hash:
                # Simple similarity based on hash comparison
                similarity = 1.0 if prev_hash == curr_hash else 0.0
            else:
                # Fallback to field-based similarity
                similarity = self._compute_field_similarity(snapshots[i-1], snapshots[i])
            
            similarities.append(similarity)
        
        return sum(similarities) / len(similarities) if similarities else 0.0
    
    def _compute_field_similarity(self, prev: Tuple, curr: Tuple) -> float:
        """Compute similarity between two snapshots based on field values"""
        fields = [1, 2, 3, 4, 5, 8, 9, 10]  # title, agency, url, description, last_modified, row_count, column_count, file_size
        matches = 0
        
        for field_idx in fields:
            if prev[field_idx] == curr[field_idx]:
                matches += 1
        
        return matches / len(fields)
    
    def _compute_license_flips(self, snapshots: List[Tuple]) -> int:
        """Count the number of license changes"""
        if len(snapshots) < 2:
            return 0
        
        flips = 0
        for i in range(1, len(snapshots)):
            prev_license = snapshots[i-1][13]  # license
            curr_license = snapshots[i][13]
            
            if prev_license and curr_license and prev_license != curr_license:
                flips += 1
        
        return flips
    
    def _compute_schema_volatility(self, snapshots: List[Tuple]) -> float:
        """Compute schema volatility based on structural changes"""
        if len(snapshots) < 2:
            return 0.0
        
        schema_changes = 0
        for i in range(1, len(snapshots)):
            prev_schema = snapshots[i-1][12]  # schema
            curr_schema = snapshots[i][12]
            
            if prev_schema and curr_schema:
                try:
                    prev_schema_data = json.loads(prev_schema)
                    curr_schema_data = json.loads(curr_schema)
                    
                    if prev_schema_data != curr_schema_data:
                        schema_changes += 1
                except (json.JSONDecodeError, TypeError):
                    # If schema parsing fails, check for changes in row/column counts
                    if (snapshots[i-1][8] != snapshots[i][8] or  # row_count
                        snapshots[i-1][9] != snapshots[i][9]):   # column_count
                        schema_changes += 1
            else:
                # Fallback to row/column count changes
                if (snapshots[i-1][8] != snapshots[i][8] or  # row_count
                    snapshots[i-1][9] != snapshots[i][9]):   # column_count
                    schema_changes += 1
        
        return schema_changes / (len(snapshots) - 1) if len(snapshots) > 1 else 0.0
    
    def _compute_availability_volatility(self, snapshots: List[Tuple]) -> float:
        """Compute availability volatility - how often availability changes"""
        if len(snapshots) < 2:
            return 0.0
        
        availability_changes = 0
        for i in range(1, len(snapshots)):
            prev_availability = snapshots[i-1][6]  # availability
            curr_availability = snapshots[i][6]
            
            if prev_availability != curr_availability:
                availability_changes += 1
        
        return availability_changes / (len(snapshots) - 1) if len(snapshots) > 1 else 0.0
    
    def get_volatility_summary(self) -> Dict:
        """Get volatility summary for all datasets"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get all unique dataset IDs
            cursor.execute('SELECT DISTINCT dataset_id FROM dataset_states')
            dataset_ids = [row[0] for row in cursor.fetchall()]
            
            volatility_data = []
            for dataset_id in dataset_ids:
                metrics = self.compute_dataset_volatility(dataset_id)
                if metrics:
                    volatility_data.append({
                        'dataset_id': metrics.dataset_id,
                        'churn_rate': metrics.churn_rate,
                        'content_drift': metrics.content_drift,
                        'license_flips': metrics.license_flips,
                        'schema_volatility': metrics.schema_volatility,
                        'availability_volatility': metrics.availability_volatility,
                        'total_changes': metrics.total_changes,
                        'avg_change_frequency': metrics.avg_change_frequency
                    })
            
            conn.close()
            
            # Compute aggregate statistics
            if volatility_data:
                return {
                    'total_datasets': len(volatility_data),
                    'avg_churn_rate': sum(d['churn_rate'] for d in volatility_data) / len(volatility_data),
                    'avg_content_drift': sum(d['content_drift'] for d in volatility_data) / len(volatility_data),
                    'total_license_flips': sum(d['license_flips'] for d in volatility_data),
                    'avg_schema_volatility': sum(d['schema_volatility'] for d in volatility_data) / len(volatility_data),
                    'avg_availability_volatility': sum(d['availability_volatility'] for d in volatility_data) / len(volatility_data),
                    'most_volatile_datasets': sorted(volatility_data, key=lambda x: x['churn_rate'], reverse=True)[:10],
                    'least_volatile_datasets': sorted(volatility_data, key=lambda x: x['churn_rate'])[:10],
                    'datasets': volatility_data
                }
            else:
                return {
                    'total_datasets': 0,
                    'avg_churn_rate': 0.0,
                    'avg_content_drift': 0.0,
                    'total_license_flips': 0,
                    'avg_schema_volatility': 0.0,
                    'avg_availability_volatility': 0.0,
                    'most_volatile_datasets': [],
                    'least_volatile_datasets': [],
                    'datasets': []
                }
                
        except Exception as e:
            logger.error(f"Error computing volatility summary: {e}")
            return {
                'total_datasets': 0,
                'avg_churn_rate': 0.0,
                'avg_content_drift': 0.0,
                'total_license_flips': 0,
                'avg_schema_volatility': 0.0,
                'avg_availability_volatility': 0.0,
                'most_volatile_datasets': [],
                'least_volatile_datasets': [],
                'datasets': []
            }
    
    def get_volatility_timeline(self, dataset_id: str) -> List[Dict]:
        """Get volatility timeline for a specific dataset"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get snapshots with time window
            cursor.execute('''
                SELECT snapshot_date, title, agency, availability, status_code,
                       row_count, column_count, file_size, content_hash, license
                FROM dataset_states
                WHERE dataset_id = ?
                ORDER BY snapshot_date ASC
            ''', (dataset_id,))
            
            snapshots = cursor.fetchall()
            if len(snapshots) < 2:
                conn.close()
                return []
            
            timeline = []
            for i in range(1, len(snapshots)):
                prev = snapshots[i-1]
                curr = snapshots[i]
                
                # Compute change indicators
                has_changes = (
                    prev[1] != curr[1] or  # title
                    prev[2] != curr[2] or  # agency
                    prev[3] != curr[3] or  # availability
                    prev[4] != curr[4] or  # status_code
                    prev[5] != curr[5] or  # row_count
                    prev[6] != curr[6] or  # column_count
                    prev[7] != curr[7] or  # file_size
                    prev[8] != curr[8] or  # content_hash
                    prev[9] != curr[9]     # license
                )
                
                timeline.append({
                    'date': curr[0],
                    'has_changes': has_changes,
                    'availability': curr[3],
                    'status_code': curr[4],
                    'row_count': curr[5],
                    'column_count': curr[6],
                    'file_size': curr[7],
                    'license': curr[9]
                })
            
            conn.close()
            return timeline
            
        except Exception as e:
            logger.error(f"Error computing volatility timeline for {dataset_id}: {e}")
            return []




