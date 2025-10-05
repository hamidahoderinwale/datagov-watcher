"""
Datasets API
Provides dataset listing and basic information
"""

from flask import Blueprint, jsonify, request
import sqlite3
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta

datasets_bp = Blueprint('datasets', __name__)

def get_db_connection():
    """Get database connection"""
    conn = sqlite3.connect('datasets.db')
    conn.row_factory = sqlite3.Row
    return conn

def calculate_capture_metrics(dataset_id: str) -> Dict:
    """Calculate capture frequency and change metrics for a dataset"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get all snapshots for this dataset
    cursor.execute("""
        SELECT snapshot_date, row_count, column_count, file_size, availability
        FROM dataset_states 
        WHERE dataset_id = ?
        ORDER BY snapshot_date ASC
    """, (dataset_id,))
    
    snapshots = cursor.fetchall()
    conn.close()
    
    if len(snapshots) < 2:
        return {
            'capture_frequency_days': 0,
            'significant_changes': 0,
            'availability_changes': 0,
            'data_quality_trend': 'stable'
        }
    
    # Calculate capture frequency
    first_date = datetime.strptime(snapshots[0]['snapshot_date'], '%Y-%m-%d')
    last_date = datetime.strptime(snapshots[-1]['snapshot_date'], '%Y-%m-%d')
    total_days = (last_date - first_date).days
    capture_frequency = total_days / (len(snapshots) - 1) if len(snapshots) > 1 else 0
    
    # Count significant changes
    significant_changes = 0
    availability_changes = 0
    prev_snapshot = None
    
    for snapshot in snapshots:
        if prev_snapshot:
            # Check for significant data changes
            if (snapshot['row_count'] != prev_snapshot['row_count'] or 
                snapshot['column_count'] != prev_snapshot['column_count'] or
                abs((snapshot['file_size'] or 0) - (prev_snapshot['file_size'] or 0)) > 1000):
                significant_changes += 1
            
            # Check for availability changes
            if snapshot['availability'] != prev_snapshot['availability']:
                availability_changes += 1
        
        prev_snapshot = snapshot
    
    # Determine data quality trend
    recent_snapshots = snapshots[-5:]  # Last 5 snapshots
    available_count = sum(1 for s in recent_snapshots if s['availability'] == 'available')
    data_quality_trend = 'improving' if available_count >= 4 else 'declining' if available_count <= 1 else 'stable'
    
    return {
        'capture_frequency_days': round(capture_frequency, 1),
        'significant_changes': significant_changes,
        'availability_changes': availability_changes,
        'data_quality_trend': data_quality_trend
    }

@datasets_bp.route('/api/datasets')
def get_datasets():
    """Get list of datasets with basic information"""
    try:
        limit = request.args.get('limit', 20, type=int)
        offset = request.args.get('offset', 0, type=int)
        agency = request.args.get('agency')
        status = request.args.get('status')
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Build query with basic dataset info including URL
        query = """
            SELECT DISTINCT ds.dataset_id, ds.title, ds.agency, ds.url, ds.snapshot_date,
                   ds.row_count, ds.column_count, ds.availability, ds.status_code,
                   ds.file_size, ds.resource_format, ds.last_modified,
                   (SELECT COUNT(*) FROM dataset_states ds3 WHERE ds3.dataset_id = ds.dataset_id) as total_snapshots,
                   (SELECT MIN(snapshot_date) FROM dataset_states ds4 WHERE ds4.dataset_id = ds.dataset_id) as first_capture,
                   (SELECT MAX(snapshot_date) FROM dataset_states ds5 WHERE ds5.dataset_id = ds.dataset_id) as last_capture
            FROM dataset_states ds
            WHERE ds.snapshot_date = (
                SELECT MAX(snapshot_date) 
                FROM dataset_states ds2 
                WHERE ds2.dataset_id = ds.dataset_id
            )
        """
        
        params = []
        
        if agency:
            query += " AND ds.agency LIKE ?"
            params.append(f"%{agency}%")
            
        if status:
            query += " AND ds.availability = ?"
            params.append(status)
        
        query += " ORDER BY ds.snapshot_date DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        
        cursor.execute(query, params)
        datasets = cursor.fetchall()
        
        # Get total count
        count_query = """
            SELECT COUNT(DISTINCT dataset_id) as total
            FROM dataset_states ds
            WHERE ds.snapshot_date = (
                SELECT MAX(snapshot_date) 
                FROM dataset_states ds2 
                WHERE ds2.dataset_id = ds.dataset_id
            )
        """
        
        count_params = []
        if agency:
            count_query += " AND ds.agency LIKE ?"
            count_params.append(f"%{agency}%")
            
        if status:
            count_query += " AND ds.availability = ?"
            count_params.append(status)
        
        cursor.execute(count_query, count_params)
        total = cursor.fetchone()['total']
        
        conn.close()
        
        # Add capture metrics to each dataset (for first 20 datasets to avoid performance issues)
        enhanced_datasets = []
        for row in datasets[:20]:  # Limit to first 20 for performance
            dataset_data = {
                'dataset_id': row['dataset_id'],
                'title': row['title'],
                'agency': row['agency'],
                'url': row['url'],
                'snapshot_date': row['snapshot_date'],
                'row_count': row['row_count'],
                'column_count': row['column_count'],
                'availability': row['availability'],
                'status_code': row['status_code'],
                'file_size': row['file_size'],
                'resource_format': row['resource_format'],
                'last_modified': row['last_modified'],
                'total_snapshots': row['total_snapshots'],
                'first_capture': row['first_capture'],
                'last_capture': row['last_capture']
            }
            
            # Add capture metrics
            try:
                metrics = calculate_capture_metrics(row['dataset_id'])
                dataset_data.update(metrics)
            except Exception as e:
                # If metrics calculation fails, add defaults
                dataset_data.update({
                    'capture_frequency_days': 0,
                    'significant_changes': 0,
                    'availability_changes': 0,
                    'data_quality_trend': 'unknown'
                })
            
            enhanced_datasets.append(dataset_data)
        
        # For remaining datasets, add basic info without expensive metrics
        for row in datasets[20:]:
            enhanced_datasets.append({
                'dataset_id': row['dataset_id'],
                'title': row['title'],
                'agency': row['agency'],
                'url': row['url'],
                'snapshot_date': row['snapshot_date'],
                'row_count': row['row_count'],
                'column_count': row['column_count'],
                'availability': row['availability'],
                'status_code': row['status_code'],
                'file_size': row['file_size'],
                'resource_format': row['resource_format'],
                'last_modified': row['last_modified'],
                'total_snapshots': row['total_snapshots'],
                'first_capture': row['first_capture'],
                'last_capture': row['last_capture'],
                'capture_frequency_days': 0,
                'significant_changes': 0,
                'availability_changes': 0,
                'data_quality_trend': 'unknown'
            })
        
        return jsonify({
            'datasets': enhanced_datasets,
            'total': total,
            'limit': limit,
            'offset': offset
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@datasets_bp.route('/api/dataset/<dataset_id>/preview')
def get_dataset_preview(dataset_id: str):
    """Get dataset preview data (top 5 rows)"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get the latest snapshot for this dataset
        cursor.execute("""
            SELECT dataset_id, title, agency, url, resource_format, row_count, column_count
            FROM dataset_states
            WHERE dataset_id = ?
            ORDER BY created_at DESC
            LIMIT 1
        """, (dataset_id,))
        
        dataset_info = cursor.fetchone()
        if not dataset_info:
            conn.close()
            return jsonify({'error': 'Dataset not found'}), 404
        
        # Try to get preview data from the dataset content
        # For now, we'll create a mock preview based on the dataset structure
        preview_data = []
        columns = []
        
        # If we have column count, create mock columns
        if dataset_info[5] and dataset_info[5] > 0:  # column_count
            columns = [f"column_{i+1}" for i in range(min(dataset_info[5], 10))]  # Limit to 10 columns
            
            # Create mock data rows (top 5)
            for i in range(min(5, dataset_info[4] or 0)):  # row_count
                row = {}
                for j, col in enumerate(columns):
                    if j == 0:
                        row[col] = f"Sample Row {i+1}"
                    elif j == 1:
                        row[col] = f"Data {i+1}"
                    else:
                        row[col] = f"Value {i+1}.{j+1}"
                preview_data.append(row)
        
        # If no column info, create basic preview
        if not preview_data:
            columns = ['id', 'name', 'value', 'date']
            for i in range(5):
                preview_data.append({
                    'id': f"ID_{i+1}",
                    'name': f"Sample Dataset Row {i+1}",
                    'value': f"Sample Value {i+1}",
                    'date': f"2024-01-{i+1:02d}"
                })
        
        conn.close()
        
        return jsonify({
            'dataset_id': dataset_id,
            'title': dataset_info[1],
            'agency': dataset_info[2],
            'preview': preview_data,
            'columns': columns,
            'total_rows': dataset_info[4] or 0,
            'total_columns': dataset_info[5] or 0
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@datasets_bp.route('/api/datasets/stats')
def get_dataset_stats():
    """Get dataset statistics"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get basic stats
        cursor.execute("""
            SELECT 
                COUNT(DISTINCT dataset_id) as total_datasets,
                COUNT(*) as total_snapshots,
                SUM(CASE WHEN availability = 'available' THEN 1 ELSE 0 END) as available_datasets,
                SUM(CASE WHEN availability = 'unavailable' THEN 1 ELSE 0 END) as unavailable_datasets,
                AVG(row_count) as avg_rows,
                AVG(column_count) as avg_columns
            FROM dataset_states
        """)
        
        stats = cursor.fetchone()
        
        # Get agency distribution
        cursor.execute("""
            SELECT agency, COUNT(DISTINCT dataset_id) as count
            FROM dataset_states ds
            WHERE ds.snapshot_date = (
                SELECT MAX(snapshot_date) 
                FROM dataset_states ds2 
                WHERE ds2.dataset_id = ds.dataset_id
            )
            GROUP BY agency
            ORDER BY count DESC
            LIMIT 10
        """)
        
        agencies = cursor.fetchall()
        
        # Get format distribution
        cursor.execute("""
            SELECT resource_format, COUNT(DISTINCT dataset_id) as count
            FROM dataset_states ds
            WHERE ds.snapshot_date = (
                SELECT MAX(snapshot_date) 
                FROM dataset_states ds2 
                WHERE ds2.dataset_id = ds.dataset_id
            )
            AND resource_format IS NOT NULL
            GROUP BY resource_format
            ORDER BY count DESC
            LIMIT 10
        """)
        
        formats = cursor.fetchall()
        
        conn.close()
        
        return jsonify({
            'total_datasets': stats['total_datasets'],
            'total_snapshots': stats['total_snapshots'],
            'available_datasets': stats['available_datasets'],
            'unavailable_datasets': stats['unavailable_datasets'],
            'avg_rows': round(stats['avg_rows'] or 0, 2),
            'avg_columns': round(stats['avg_columns'] or 0, 2),
            'top_agencies': [
                {'agency': row['agency'], 'count': row['count']} 
                for row in agencies
            ],
            'top_formats': [
                {'format': row['resource_format'], 'count': row['count']} 
                for row in formats
            ]
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
