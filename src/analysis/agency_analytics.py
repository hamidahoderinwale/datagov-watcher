"""
Agency Analytics: Comprehensive agency comparison and analysis
"""

import sqlite3
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from pathlib import Path

class AgencyAnalytics:
    def __init__(self, db_path: str = "datasets.db"):
        self.db_path = db_path
    
    def get_agency_comparison(self, days: int = 30) -> Dict[str, Any]:
        """Get comprehensive agency comparison data"""
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get agency statistics from latest snapshots
        cursor.execute('''
            SELECT 
                ds.agency,
                COUNT(*) as total_datasets,
                SUM(CASE WHEN ds.availability = 'available' THEN 1 ELSE 0 END) as available_datasets,
                SUM(CASE WHEN ds.availability = 'unavailable' THEN 1 ELSE 0 END) as unavailable_datasets,
                AVG(ds.row_count) as avg_rows,
                AVG(ds.column_count) as avg_columns,
                SUM(ds.file_size) as total_file_size,
                AVG(ds.file_size) as avg_file_size,
                COUNT(DISTINCT ds.resource_format) as format_diversity,
                MAX(ds.created_at) as last_updated
            FROM dataset_states ds
            INNER JOIN (
                SELECT dataset_id, MAX(created_at) as max_created
                FROM dataset_states 
                GROUP BY dataset_id
            ) latest ON ds.dataset_id = latest.dataset_id 
            AND ds.created_at = latest.max_created
            WHERE ds.agency IS NOT NULL AND ds.agency != ''
            GROUP BY ds.agency
            ORDER BY total_datasets DESC
        ''')
        
        agencies = []
        for row in cursor.fetchall():
            agency, total, available, unavailable, avg_rows, avg_cols, total_size, avg_size, format_diversity, last_updated = row
            
            availability_rate = (available / total * 100) if total > 0 else 0
            
            agencies.append({
                'agency': agency,
                'total_datasets': total,
                'available_datasets': available,
                'unavailable_datasets': unavailable,
                'availability_rate': round(availability_rate, 1),
                'avg_rows': round(avg_rows or 0, 0),
                'avg_columns': round(avg_cols or 0, 1),
                'total_file_size': total_size or 0,
                'avg_file_size': round(avg_size or 0, 0),
                'format_diversity': format_diversity,
                'last_updated': last_updated
            })
        
        # Get agency trends over time
        cursor.execute('''
            SELECT 
                ds.agency,
                ds.snapshot_date,
                COUNT(*) as daily_datasets,
                SUM(CASE WHEN ds.availability = 'available' THEN 1 ELSE 0 END) as daily_available
            FROM dataset_states ds
            WHERE ds.agency IS NOT NULL 
            AND ds.agency != ''
            AND ds.snapshot_date >= date('now', '-{} days')
            GROUP BY ds.agency, ds.snapshot_date
            ORDER BY ds.agency, ds.snapshot_date
        '''.format(days))
        
        agency_trends = {}
        for row in cursor.fetchall():
            agency, date, daily_total, daily_available = row
            if agency not in agency_trends:
                agency_trends[agency] = []
            agency_trends[agency].append({
                'date': date,
                'total_datasets': daily_total,
                'available_datasets': daily_available,
                'availability_rate': round((daily_available / daily_total * 100) if daily_total > 0 else 0, 1)
            })
        
        # Get agency format distribution
        cursor.execute('''
            SELECT 
                ds.agency,
                ds.resource_format,
                COUNT(*) as count
            FROM dataset_states ds
            INNER JOIN (
                SELECT dataset_id, MAX(created_at) as max_created
                FROM dataset_states 
                GROUP BY dataset_id
            ) latest ON ds.dataset_id = latest.dataset_id 
            AND ds.created_at = latest.max_created
            WHERE ds.agency IS NOT NULL 
            AND ds.agency != ''
            AND ds.resource_format IS NOT NULL
            AND ds.resource_format != ''
            GROUP BY ds.agency, ds.resource_format
            ORDER BY ds.agency, count DESC
        ''')
        
        agency_formats = {}
        for row in cursor.fetchall():
            agency, format_type, count = row
            if agency not in agency_formats:
                agency_formats[agency] = []
            agency_formats[agency].append({
                'format': format_type,
                'count': count
            })
        
        conn.close()
        
        return {
            'agencies': agencies,
            'trends': agency_trends,
            'formats': agency_formats,
            'summary': {
                'total_agencies': len(agencies),
                'top_agency': agencies[0]['agency'] if agencies else 'N/A',
                'avg_availability': round(sum(a['availability_rate'] for a in agencies) / len(agencies), 1) if agencies else 0,
                'total_datasets': sum(a['total_datasets'] for a in agencies)
            }
        }
    
    def get_agency_leaderboard(self, metric: str = 'total_datasets', limit: int = 10) -> List[Dict]:
        """Get agency leaderboard by various metrics"""
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Define metric mappings
        metric_queries = {
            'total_datasets': 'COUNT(*)',
            'availability_rate': 'ROUND((SUM(CASE WHEN ds.availability = "available" THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 1)',
            'avg_rows': 'ROUND(AVG(ds.row_count), 0)',
            'avg_columns': 'ROUND(AVG(ds.column_count), 1)',
            'total_file_size': 'SUM(ds.file_size)',
            'format_diversity': 'COUNT(DISTINCT ds.resource_format)'
        }
        
        if metric not in metric_queries:
            metric = 'total_datasets'
        
        cursor.execute(f'''
            SELECT 
                ds.agency,
                {metric_queries[metric]} as metric_value,
                COUNT(*) as total_datasets,
                SUM(CASE WHEN ds.availability = 'available' THEN 1 ELSE 0 END) as available_datasets,
                ROUND((SUM(CASE WHEN ds.availability = 'available' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 1) as availability_rate
            FROM dataset_states ds
            INNER JOIN (
                SELECT dataset_id, MAX(created_at) as max_created
                FROM dataset_states 
                GROUP BY dataset_id
            ) latest ON ds.dataset_id = latest.dataset_id 
            AND ds.created_at = latest.max_created
            WHERE ds.agency IS NOT NULL AND ds.agency != ''
            GROUP BY ds.agency
            ORDER BY metric_value DESC
            LIMIT ?
        ''', (limit,))
        
        leaderboard = []
        for i, row in enumerate(cursor.fetchall(), 1):
            agency, metric_value, total, available, availability_rate = row
            leaderboard.append({
                'rank': i,
                'agency': agency,
                'metric_value': metric_value,
                'total_datasets': total,
                'available_datasets': available,
                'availability_rate': availability_rate
            })
        
        conn.close()
        return leaderboard
    
    def get_agency_insights(self, agency: str) -> Dict[str, Any]:
        """Get detailed insights for a specific agency"""
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get agency overview
        cursor.execute('''
            SELECT 
                COUNT(*) as total_datasets,
                SUM(CASE WHEN ds.availability = 'available' THEN 1 ELSE 0 END) as available_datasets,
                AVG(ds.row_count) as avg_rows,
                AVG(ds.column_count) as avg_columns,
                SUM(ds.file_size) as total_file_size,
                COUNT(DISTINCT ds.resource_format) as format_diversity
            FROM dataset_states ds
            INNER JOIN (
                SELECT dataset_id, MAX(created_at) as max_created
                FROM dataset_states 
                GROUP BY dataset_id
            ) latest ON ds.dataset_id = latest.dataset_id 
            AND ds.created_at = latest.max_created
            WHERE ds.agency = ?
        ''', (agency,))
        
        overview = cursor.fetchone()
        if not overview:
            return {'error': 'Agency not found'}
        
        total, available, avg_rows, avg_cols, total_size, format_diversity = overview
        availability_rate = (available / total * 100) if total > 0 else 0
        
        # Get format distribution
        cursor.execute('''
            SELECT 
                ds.resource_format,
                COUNT(*) as count,
                AVG(ds.row_count) as avg_rows,
                AVG(ds.file_size) as avg_size
            FROM dataset_states ds
            INNER JOIN (
                SELECT dataset_id, MAX(created_at) as max_created
                FROM dataset_states 
                GROUP BY dataset_id
            ) latest ON ds.dataset_id = latest.dataset_id 
            AND ds.created_at = latest.max_created
            WHERE ds.agency = ?
            AND ds.resource_format IS NOT NULL
            AND ds.resource_format != ''
            GROUP BY ds.resource_format
            ORDER BY count DESC
        ''', (agency,))
        
        formats = []
        for row in cursor.fetchall():
            format_type, count, avg_rows, avg_size = row
            formats.append({
                'format': format_type,
                'count': count,
                'avg_rows': round(avg_rows or 0, 0),
                'avg_size': round(avg_size or 0, 0)
            })
        
        # Get recent activity
        cursor.execute('''
            SELECT 
                ds.snapshot_date,
                COUNT(*) as daily_datasets,
                SUM(CASE WHEN ds.availability = 'available' THEN 1 ELSE 0 END) as daily_available
            FROM dataset_states ds
            WHERE ds.agency = ?
            AND ds.snapshot_date >= date('now', '-30 days')
            GROUP BY ds.snapshot_date
            ORDER BY ds.snapshot_date DESC
        ''', (agency,))
        
        recent_activity = []
        for row in cursor.fetchall():
            date, daily_total, daily_available = row
            recent_activity.append({
                'date': date,
                'total_datasets': daily_total,
                'available_datasets': daily_available,
                'availability_rate': round((daily_available / daily_total * 100) if daily_total > 0 else 0, 1)
            })
        
        conn.close()
        
        return {
            'agency': agency,
            'overview': {
                'total_datasets': total,
                'available_datasets': available,
                'availability_rate': round(availability_rate, 1),
                'avg_rows': round(avg_rows or 0, 0),
                'avg_columns': round(avg_cols or 0, 1),
                'total_file_size': total_size or 0,
                'format_diversity': format_diversity
            },
            'formats': formats,
            'recent_activity': recent_activity
        }


