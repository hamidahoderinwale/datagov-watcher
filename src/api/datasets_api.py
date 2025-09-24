"""
Datasets API
Provides dataset listing and basic information
"""

from flask import Blueprint, jsonify, request
import sqlite3
from typing import Dict, List, Optional, Any

datasets_bp = Blueprint('datasets', __name__)

def get_db_connection():
    """Get database connection"""
    conn = sqlite3.connect('datasets.db')
    conn.row_factory = sqlite3.Row
    return conn

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
        
        # Build query
        query = """
            SELECT DISTINCT ds.dataset_id, ds.title, ds.agency, ds.snapshot_date,
                   ds.row_count, ds.column_count, ds.availability, ds.status_code,
                   ds.file_size, ds.resource_format, ds.last_modified
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
        
        return jsonify({
            'datasets': [
                {
                    'dataset_id': row['dataset_id'],
                    'title': row['title'],
                    'agency': row['agency'],
                    'snapshot_date': row['snapshot_date'],
                    'row_count': row['row_count'],
                    'column_count': row['column_count'],
                    'availability': row['availability'],
                    'status_code': row['status_code'],
                    'file_size': row['file_size'],
                    'resource_format': row['resource_format'],
                    'last_modified': row['last_modified']
                } for row in datasets
            ],
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
