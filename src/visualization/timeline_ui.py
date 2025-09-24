"""
Timeline UI Components for Dataset State Historian
Implements chromogram visualization and comprehensive timeline views
"""

import json
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import logging
from pathlib import Path
import hashlib
import colorsys

logger = logging.getLogger(__name__)

class TimelineUI:
    """Timeline UI components with chromogram visualization"""
    
    def __init__(self, db_path: str = "datasets.db"):
        self.db_path = db_path
        self.color_palette = self._generate_color_palette()
    
    def _generate_color_palette(self) -> Dict[str, str]:
        """Generate a consistent color palette for field visualization"""
        # Generate colors using HSL for better contrast
        colors = {}
        field_types = [
            'title', 'agency', 'publisher', 'license', 'landing_page', 'modified',
            'column_added', 'column_removed', 'column_renamed', 'row_count',
            'content_drift', 'similarity', 'availability'
        ]
        
        for i, field in enumerate(field_types):
            hue = (i * 137.5) % 360  # Golden angle for good distribution
            saturation = 0.7
            lightness = 0.5
            rgb = colorsys.hls_to_rgb(hue/360, lightness, saturation)
            hex_color = f"#{int(rgb[0]*255):02x}{int(rgb[1]*255):02x}{int(rgb[2]*255):02x}"
            colors[field] = hex_color
        
        return colors
    
    def generate_chromogram_data(self, dataset_id: str) -> Dict:
        """Generate chromogram data for a dataset timeline including vanished states"""
        try:
            # Get dataset timeline
            timeline = self._get_dataset_timeline(dataset_id)
            if not timeline:
                return {'error': 'No timeline data available'}
            
            # Extract all unique fields across timeline
            all_fields = set()
            for snapshot in timeline:
                for change in snapshot.get('changes', []):
                    all_fields.add(change.get('field', 'unknown'))
            
            # Generate chromogram matrix
            chromogram_data = {
                'dataset_id': dataset_id,
                'fields': sorted(list(all_fields)),
                'timeline': [],
                'color_map': self.color_palette,
                'dataset_status': 'active'  # Will be updated based on timeline
            }
            
            for snapshot in timeline:
                snapshot_data = {
                    'date': snapshot['date'],
                    'source': snapshot.get('source', 'unknown'),
                    'status': snapshot.get('status', 'active'),
                    'provenance': snapshot.get('provenance', {}),
                    'field_states': {}
                }
                
                # Initialize all fields as unchanged
                for field in all_fields:
                    snapshot_data['field_states'][field] = {
                        'changed': False,
                        'value': None,
                        'color': self.color_palette.get(field, '#cccccc')
                    }
                
                # Mark changed fields
                for change in snapshot.get('changes', []):
                    field = change.get('field', 'unknown')
                    if field in snapshot_data['field_states']:
                        snapshot_data['field_states'][field]['changed'] = True
                        snapshot_data['field_states'][field]['value'] = change.get('new_value')
                
                # Update dataset status
                if snapshot.get('status') in ['vanished', 'archived']:
                    chromogram_data['dataset_status'] = snapshot.get('status')
                
                chromogram_data['timeline'].append(snapshot_data)
            
            return chromogram_data
            
        except Exception as e:
            logger.error(f"Error generating chromogram data for {dataset_id}: {e}")
            return {'error': str(e)}
    
    def _get_dataset_timeline(self, dataset_id: str) -> List[Dict]:
        """Get timeline data for a dataset"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get snapshots
        cursor.execute('''
            SELECT snapshot_date, title, agency, publisher, license, 
                   landing_page, modified, created_at
            FROM historian_snapshots
            WHERE dataset_id = ?
            ORDER BY snapshot_date ASC
        ''', (dataset_id,))
        
        snapshots = []
        for row in cursor.fetchall():
            snapshots.append({
                'date': row[0],
                'title': row[1],
                'agency': row[2],
                'publisher': row[3],
                'license': row[4],
                'landing_page': row[5],
                'modified': row[6],
                'created_at': row[7],
                'changes': []
            })
        
        # Get diffs to populate changes
        cursor.execute('''
            SELECT from_date, to_date, metadata_changes, schema_changes, 
                   content_changes, volatility_score, change_events
            FROM historian_diffs
            WHERE dataset_id = ?
            ORDER BY from_date ASC
        ''', (dataset_id,))
        
        diffs = cursor.fetchall()
        conn.close()
        
        # Map changes to snapshots
        for diff in diffs:
            from_date, to_date, metadata_changes, schema_changes, content_changes, volatility, events = diff
            
            # Find the target snapshot
            target_snapshot = None
            for snapshot in snapshots:
                if snapshot['date'] == to_date:
                    target_snapshot = snapshot
                    break
            
            if target_snapshot:
                # Add metadata changes
                if metadata_changes:
                    changes = json.loads(metadata_changes)
                    for change in changes:
                        target_snapshot['changes'].append({
                            'field': change.get('field', 'unknown'),
                            'old_value': change.get('old_value'),
                            'new_value': change.get('new_value'),
                            'change_type': change.get('change_type', 'metadata_update')
                        })
                
                # Add schema changes
                if schema_changes:
                    changes = json.loads(schema_changes)
                    for change in changes:
                        target_snapshot['changes'].append({
                            'field': change.get('field', 'unknown'),
                            'column_name': change.get('column_name'),
                            'old_value': change.get('old_value'),
                            'new_value': change.get('new_value'),
                            'change_type': change.get('change_type', 'schema_change')
                        })
                
                # Add content changes
                if content_changes:
                    changes = json.loads(content_changes)
                    if 'content_drift' in changes:
                        target_snapshot['changes'].append({
                            'field': 'content_drift',
                            'old_value': None,
                            'new_value': changes['content_drift'],
                            'change_type': 'content_change'
                        })
                
                # Add volatility
                target_snapshot['volatility'] = volatility
        
        return snapshots
    
    def generate_timeline_summary(self, dataset_id: str) -> Dict:
        """Generate a comprehensive timeline summary"""
        try:
            timeline = self._get_dataset_timeline(dataset_id)
            if not timeline:
                return {'error': 'No timeline data available'}
            
            # Calculate summary statistics
            total_snapshots = len(timeline)
            total_changes = sum(len(snapshot.get('changes', [])) for snapshot in timeline)
            avg_volatility = sum(snapshot.get('volatility', 0) for snapshot in timeline) / total_snapshots
            
            # Find first and last seen
            first_seen = timeline[0]['date'] if timeline else None
            last_seen = timeline[-1]['date'] if timeline else None
            
            # Count change types
            change_types = {}
            for snapshot in timeline:
                for change in snapshot.get('changes', []):
                    change_type = change.get('change_type', 'unknown')
                    change_types[change_type] = change_types.get(change_type, 0) + 1
            
            # Find significant events
            significant_events = []
            for snapshot in timeline:
                if snapshot.get('volatility', 0) > 0.5:  # High volatility threshold
                    significant_events.append({
                        'date': snapshot['date'],
                        'volatility': snapshot.get('volatility', 0),
                        'changes': len(snapshot.get('changes', []))
                    })
            
            return {
                'dataset_id': dataset_id,
                'summary': {
                    'total_snapshots': total_snapshots,
                    'total_changes': total_changes,
                    'avg_volatility': round(avg_volatility, 3),
                    'first_seen': first_seen,
                    'last_seen': last_seen,
                    'change_types': change_types,
                    'significant_events': significant_events
                },
                'timeline': timeline
            }
            
        except Exception as e:
            logger.error(f"Error generating timeline summary for {dataset_id}: {e}")
            return {'error': str(e)}
    
    def generate_field_diff_panel(self, dataset_id: str, field_name: str) -> Dict:
        """Generate detailed field diff panel"""
        try:
            timeline = self._get_dataset_timeline(dataset_id)
            if not timeline:
                return {'error': 'No timeline data available'}
            
            field_history = []
            current_value = None
            
            for snapshot in timeline:
                # Find changes for this field
                field_changes = [c for c in snapshot.get('changes', []) if c.get('field') == field_name]
                
                if field_changes:
                    for change in field_changes:
                        field_history.append({
                            'date': snapshot['date'],
                            'old_value': change.get('old_value'),
                            'new_value': change.get('new_value'),
                            'change_type': change.get('change_type'),
                            'volatility': snapshot.get('volatility', 0)
                        })
                        current_value = change.get('new_value')
                elif current_value is not None:
                    # Field unchanged, show current value
                    field_history.append({
                        'date': snapshot['date'],
                        'old_value': current_value,
                        'new_value': current_value,
                        'change_type': 'unchanged',
                        'volatility': snapshot.get('volatility', 0)
                    })
            
            return {
                'dataset_id': dataset_id,
                'field_name': field_name,
                'current_value': current_value,
                'history': field_history,
                'change_count': len([h for h in field_history if h['change_type'] != 'unchanged'])
            }
            
        except Exception as e:
            logger.error(f"Error generating field diff panel for {dataset_id}, {field_name}: {e}")
            return {'error': str(e)}
    
    def generate_content_drift_panel(self, dataset_id: str) -> Dict:
        """Generate content drift analysis panel"""
        try:
            timeline = self._get_dataset_timeline(dataset_id)
            if not timeline:
                return {'error': 'No timeline data available'}
            
            drift_data = []
            similarity_data = []
            
            for snapshot in timeline:
                # Extract content drift information
                content_changes = [c for c in snapshot.get('changes', []) if c.get('field') == 'content_drift']
                
                if content_changes:
                    drift_value = content_changes[0].get('new_value', 0)
                    similarity_value = 1.0 - drift_value  # Convert drift to similarity
                else:
                    drift_value = 0.0
                    similarity_value = 1.0
                
                drift_data.append({
                    'date': snapshot['date'],
                    'drift': drift_value,
                    'similarity': similarity_value,
                    'volatility': snapshot.get('volatility', 0)
                })
                
                similarity_data.append({
                    'date': snapshot['date'],
                    'similarity': similarity_value
                })
            
            # Calculate drift statistics
            drift_values = [d['drift'] for d in drift_data]
            max_drift = max(drift_values) if drift_values else 0
            avg_drift = sum(drift_values) / len(drift_values) if drift_values else 0
            
            # Find drift events
            drift_events = [d for d in drift_data if d['drift'] > 0.15]  # 15% threshold
            
            return {
                'dataset_id': dataset_id,
                'drift_data': drift_data,
                'similarity_data': similarity_data,
                'statistics': {
                    'max_drift': round(max_drift, 3),
                    'avg_drift': round(avg_drift, 3),
                    'drift_events': len(drift_events)
                },
                'drift_events': drift_events
            }
            
        except Exception as e:
            logger.error(f"Error generating content drift panel for {dataset_id}: {e}")
            return {'error': str(e)}
    
    def generate_change_log(self, dataset_id: str) -> List[Dict]:
        """Generate comprehensive change log"""
        try:
            timeline = self._get_dataset_timeline(dataset_id)
            if not timeline:
                return []
            
            change_log = []
            
            for snapshot in timeline:
                for change in snapshot.get('changes', []):
                    change_log.append({
                        'date': snapshot['date'],
                        'field': change.get('field', 'unknown'),
                        'change_type': change.get('change_type', 'unknown'),
                        'old_value': change.get('old_value'),
                        'new_value': change.get('new_value'),
                        'volatility': snapshot.get('volatility', 0),
                        'severity': self._calculate_change_severity(change, snapshot.get('volatility', 0))
                    })
            
            # Sort by date (most recent first)
            change_log.sort(key=lambda x: x['date'], reverse=True)
            
            return change_log
            
        except Exception as e:
            logger.error(f"Error generating change log for {dataset_id}: {e}")
            return []
    
    def _calculate_change_severity(self, change: Dict, volatility: float) -> str:
        """Calculate change severity based on type and volatility"""
        change_type = change.get('change_type', 'unknown')
        
        # High severity changes
        if change_type in ['license_change', 'column_removed']:
            return 'high'
        
        # Medium severity changes
        if change_type in ['schema_change', 'content_change', 'publisher_change']:
            return 'medium'
        
        # Low severity changes
        if change_type in ['metadata_update', 'row_count_change']:
            return 'low'
        
        # Use volatility as tiebreaker
        if volatility > 0.7:
            return 'high'
        elif volatility > 0.3:
            return 'medium'
        else:
            return 'low'
    
    def generate_volatility_ranking(self, limit: int = 50) -> List[Dict]:
        """Generate volatility ranking across all datasets"""
        try:
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
            
            ranking = []
            for row in cursor.fetchall():
                ranking.append({
                    'dataset_id': row[0],
                    'volatility_score': round(row[1], 3),
                    'change_count': row[2],
                    'last_change': row[3],
                    'risk_level': self._calculate_risk_level(row[1])
                })
            
            conn.close()
            return ranking
            
        except Exception as e:
            logger.error(f"Error generating volatility ranking: {e}")
            return []
    
    def _calculate_risk_level(self, volatility: float) -> str:
        """Calculate risk level based on volatility score"""
        if volatility > 0.7:
            return 'high'
        elif volatility > 0.4:
            return 'medium'
        else:
            return 'low'
    
    def generate_dataset_overview(self, dataset_id: str) -> Dict:
        """Generate comprehensive dataset overview"""
        try:
            # Get basic timeline
            timeline_summary = self.generate_timeline_summary(dataset_id)
            if 'error' in timeline_summary:
                return timeline_summary
            
            # Get volatility ranking
            ranking = self.generate_volatility_ranking(1000)  # Get all datasets
            rank_position = None
            for i, item in enumerate(ranking):
                if item['dataset_id'] == dataset_id:
                    rank_position = i + 1
                    break
            
            # Get recent changes
            change_log = self.generate_change_log(dataset_id)
            recent_changes = change_log[:10]  # Last 10 changes
            
            # Get content drift info
            drift_panel = self.generate_content_drift_panel(dataset_id)
            
            return {
                'dataset_id': dataset_id,
                'timeline_summary': timeline_summary['summary'],
                'volatility_rank': rank_position,
                'recent_changes': recent_changes,
                'content_drift': drift_panel.get('statistics', {}),
                'risk_level': self._calculate_risk_level(timeline_summary['summary']['avg_volatility'])
            }
            
        except Exception as e:
            logger.error(f"Error generating dataset overview for {dataset_id}: {e}")
            return {'error': str(e)}
    
    def generate_vanished_datasets_overview(self) -> Dict:
        """Generate overview of all vanished datasets"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get vanished datasets with statistics
            cursor.execute('''
                SELECT vd.dataset_id, vd.last_seen_date, vd.last_seen_source, 
                       vd.disappearance_date, vd.last_known_title, vd.last_known_agency,
                       vd.archival_sources, vd.status,
                       COUNT(hs.id) as archived_snapshots,
                       AVG(hd.volatility_score) as avg_volatility
                FROM vanished_datasets vd
                LEFT JOIN historian_snapshots hs ON vd.dataset_id = hs.dataset_id 
                    AND hs.source IN ('wayback', 'eota', 'lil')
                LEFT JOIN historian_diffs hd ON vd.dataset_id = hd.dataset_id
                GROUP BY vd.dataset_id
                ORDER BY vd.disappearance_date DESC
            ''')
            
            vanished_datasets = []
            for row in cursor.fetchall():
                vanished_datasets.append({
                    'dataset_id': row[0],
                    'last_seen_date': row[1],
                    'last_seen_source': row[2],
                    'disappearance_date': row[3],
                    'last_known_title': row[4],
                    'last_known_agency': row[5],
                    'archival_sources': json.loads(row[6]) if row[6] else [],
                    'status': row[7],
                    'archived_snapshots': row[8],
                    'avg_volatility': round(row[9], 3) if row[9] else 0.0
                })
            
            # Calculate summary statistics
            total_vanished = len(vanished_datasets)
            with_archives = len([vd for vd in vanished_datasets if vd['archived_snapshots'] > 0])
            avg_volatility = sum(vd['avg_volatility'] for vd in vanished_datasets) / total_vanished if total_vanished > 0 else 0
            
            # Group by agency
            agency_counts = {}
            for vd in vanished_datasets:
                agency = vd['last_known_agency']
                agency_counts[agency] = agency_counts.get(agency, 0) + 1
            
            # Group by disappearance date
            monthly_disappearances = {}
            for vd in vanished_datasets:
                if vd['disappearance_date']:
                    month = vd['disappearance_date'][:7]  # YYYY-MM
                    monthly_disappearances[month] = monthly_disappearances.get(month, 0) + 1
            
            conn.close()
            
            return {
                'summary': {
                    'total_vanished': total_vanished,
                    'with_archives': with_archives,
                    'archive_coverage': round(with_archives / total_vanished * 100, 1) if total_vanished > 0 else 0,
                    'avg_volatility': round(avg_volatility, 3),
                    'top_agencies': sorted(agency_counts.items(), key=lambda x: x[1], reverse=True)[:10],
                    'monthly_disappearances': monthly_disappearances
                },
                'vanished_datasets': vanished_datasets
            }
            
        except Exception as e:
            logger.error(f"Error generating vanished datasets overview: {e}")
            return {'error': str(e)}


