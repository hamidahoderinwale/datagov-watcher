"""
Charts API - Data for dashboard visualizations
"""

from flask import Blueprint, jsonify
import sqlite3
from datetime import datetime, timedelta
from collections import defaultdict

charts_bp = Blueprint('charts', __name__, url_prefix='/api/charts')

def get_db_connection():
    """Get database connection"""
    return sqlite3.connect('datasets.db')

@charts_bp.route('/status-over-time')
def get_status_over_time():
    """Get dataset status data over time for charting"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get status data over the last 30 days
        thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        cursor.execute("""
            SELECT 
                snapshot_date,
                COALESCE(availability, 'unknown') as status,
                COUNT(*) as count
            FROM dataset_states 
            WHERE snapshot_date >= ?
            GROUP BY snapshot_date, COALESCE(availability, 'unknown')
            ORDER BY snapshot_date ASC
        """, (thirty_days_ago,))
        
        results = cursor.fetchall()
        conn.close()
        
        # Process data for chart
        status_data = defaultdict(lambda: defaultdict(int))
        for date, status, count in results:
            status_data[date][status] = count
        
        # Convert to chart format
        dates = sorted(status_data.keys())
        available_data = [status_data[date].get('available', 0) for date in dates]
        unavailable_data = [status_data[date].get('unavailable', 0) for date in dates]
        unknown_data = [status_data[date].get('unknown', 0) for date in dates]
        
        return jsonify({
            'labels': dates,
            'datasets': [
                {
                    'label': 'Available',
                    'data': available_data,
                    'backgroundColor': '#4a4a4a',
                    'borderColor': '#2a2a2a'
                },
                {
                    'label': 'Unavailable',
                    'data': unavailable_data,
                    'backgroundColor': '#7a7a7a',
                    'borderColor': '#5a5a5a'
                },
                {
                    'label': 'Unknown',
                    'data': unknown_data,
                    'backgroundColor': '#a8a39c',
                    'borderColor': '#8a8a8a'
                }
            ]
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@charts_bp.route('/agency-distribution')
def get_agency_distribution():
    """Get agency distribution data for charting"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get agency distribution from latest snapshots
        cursor.execute("""
            SELECT 
                COALESCE(agency, 'Unknown') as agency,
                COUNT(*) as count
            FROM dataset_states ds
            WHERE ds.id IN (
                SELECT MAX(id) 
                FROM dataset_states 
                GROUP BY dataset_id
            )
            GROUP BY COALESCE(agency, 'Unknown')
            ORDER BY count DESC
            LIMIT 20
        """)
        
        results = cursor.fetchall()
        conn.close()
        
        # Process data for chart
        agencies = [row[0] for row in results]
        counts = [row[1] for row in results]
        
        # Generate colors for each agency
        colors = [
            '#4a4a4a', '#7a7a7a', '#a8a39c', '#d6d1c9', '#e8e5e0',
            '#5c5750', '#3e3a35', '#2a2621', '#1a1612', '#374151',
            '#6b7280', '#111827', '#4b5563', '#9ca3af', '#d1d5db',
            '#f3f4f6', '#e5e7eb', '#d1d5db', '#9ca3af', '#6b7280'
        ]
        
        return jsonify({
            'labels': agencies,
            'datasets': [{
                'label': 'Datasets',
                'data': counts,
                'backgroundColor': colors[:len(agencies)],
                'borderColor': '#2a2a2a',
                'borderWidth': 1
            }]
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@charts_bp.route('/format-distribution')
def get_format_distribution():
    """Get data format distribution for charting"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get format distribution from latest snapshots
        cursor.execute("""
            SELECT 
                COALESCE(resource_format, 'Unknown') as format,
                COUNT(*) as count
            FROM dataset_states ds
            WHERE ds.id IN (
                SELECT MAX(id) 
                FROM dataset_states 
                GROUP BY dataset_id
            )
            GROUP BY COALESCE(resource_format, 'Unknown')
            ORDER BY count DESC
            LIMIT 15
        """)
        
        results = cursor.fetchall()
        conn.close()
        
        # Process data for chart
        formats = [row[0] for row in results]
        counts = [row[1] for row in results]
        
        return jsonify({
            'labels': formats,
            'datasets': [{
                'label': 'Datasets',
                'data': counts,
                'backgroundColor': '#4a4a4a',
                'borderColor': '#2a2a2a'
            }]
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@charts_bp.route('/quality-trends')
def get_quality_trends():
    """Get data quality trends over time"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get quality trends over the last 30 days
        thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        cursor.execute("""
            SELECT 
                snapshot_date,
                AVG(CASE WHEN analysis_quality_score IS NOT NULL THEN analysis_quality_score ELSE 0 END) as avg_quality,
                COUNT(CASE WHEN analysis_quality_score IS NOT NULL THEN 1 END) as quality_count,
                COUNT(*) as total_count
            FROM dataset_states 
            WHERE snapshot_date >= ?
            GROUP BY snapshot_date
            ORDER BY snapshot_date ASC
        """, (thirty_days_ago,))
        
        results = cursor.fetchall()
        conn.close()
        
        # Process data for chart
        dates = [row[0] for row in results]
        avg_quality = [round(row[1], 1) for row in results]
        quality_coverage = [round((row[2] / row[3]) * 100, 1) if row[3] > 0 else 0 for row in results]
        
        return jsonify({
            'labels': dates,
            'datasets': [
                {
                    'label': 'Average Quality Score',
                    'data': avg_quality,
                    'backgroundColor': 'rgba(74, 74, 74, 0.2)',
                    'borderColor': '#4a4a4a',
                    'yAxisID': 'y'
                },
                {
                    'label': 'Quality Coverage (%)',
                    'data': quality_coverage,
                    'backgroundColor': 'rgba(122, 122, 122, 0.2)',
                    'borderColor': '#7a7a7a',
                    'yAxisID': 'y1'
                }
            ]
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
