"""
Dataset Detail API
Provides comprehensive dataset information and metadata
"""

from flask import Blueprint, jsonify, request
import sqlite3
import json
from datetime import datetime
from typing import Dict, List, Optional, Any

dataset_detail_bp = Blueprint('dataset_detail', __name__)

def get_db_connection():
    """Get database connection"""
    conn = sqlite3.connect('datasets.db')
    conn.row_factory = sqlite3.Row
    return conn

@dataset_detail_bp.route('/api/dataset/<dataset_id>')
def get_dataset_detail(dataset_id: str):
    """Get comprehensive dataset information"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get latest dataset state
        cursor.execute("""
            SELECT ds.*, d.title, d.agency, d.url, d.description, d.last_modified, d.source
            FROM dataset_states ds
            LEFT JOIN datasets d ON ds.dataset_id = d.id
            WHERE ds.dataset_id = ?
            ORDER BY ds.snapshot_date DESC
            LIMIT 1
        """, (dataset_id,))
        
        dataset = cursor.fetchone()
        if not dataset:
            return jsonify({'error': 'Dataset not found'}), 404
        
        # Get historical states
        cursor.execute("""
            SELECT snapshot_date, row_count, column_count, file_size, status_code, 
                   content_hash, schema_hash, availability, last_modified
            FROM dataset_states
            WHERE dataset_id = ?
            ORDER BY snapshot_date DESC
            LIMIT 50
        """, (dataset_id,))
        
        history = cursor.fetchall()
        
        # Get change events
        cursor.execute("""
            SELECT change_type, change_description, change_date, old_value, new_value
            FROM dataset_changes
            WHERE dataset_id = ?
            ORDER BY change_date DESC
            LIMIT 20
        """, (dataset_id,))
        
        changes = cursor.fetchall()
        
        # Get quality metrics
        cursor.execute("""
            SELECT analysis_quality_score, content_analyzed, schema_columns, schema_dtypes
            FROM dataset_states
            WHERE dataset_id = ? AND analysis_quality_score IS NOT NULL
            ORDER BY snapshot_date DESC
            LIMIT 1
        """, (dataset_id,))
        
        quality = cursor.fetchone()
        
        # Get agency information
        agency_info = {}
        if dataset['agency']:
            cursor.execute("""
                SELECT COUNT(*) as dataset_count, 
                       AVG(analysis_quality_score) as avg_quality,
                       MAX(snapshot_date) as last_update
                FROM dataset_states ds
                LEFT JOIN datasets d ON ds.dataset_id = d.id
                WHERE d.agency = ?
            """, (dataset['agency'],))
            
            agency_stats = cursor.fetchone()
            if agency_stats:
                agency_info = {
                    'dataset_count': agency_stats['dataset_count'],
                    'avg_quality': round(agency_stats['avg_quality'] or 0, 2),
                    'last_update': agency_stats['last_update']
                }
        
        conn.close()
        
        # Format response
        result = {
            'dataset_id': dataset_id,
            'title': dataset['title'] or 'Unknown Title',
            'agency': dataset['agency'] or 'Unknown Agency',
            'url': dataset['url'],
            'description': dataset['description'],
            'tags': [],  # Tags not available in current schema
            'license': None,  # License not available in current schema
            'publisher': dataset['source'],  # Using source as publisher
            'current_state': {
                'snapshot_date': dataset['snapshot_date'],
                'row_count': dataset['row_count'],
                'column_count': dataset['column_count'],
                'file_size': dataset['file_size'],
                'status_code': dataset['status_code'],
                'availability': dataset['availability'],
                'content_type': dataset['content_type'],
                'resource_format': dataset['resource_format'],
                'last_modified': dataset['last_modified'],
                'dimensions_computed': bool(dataset['dimensions_computed']),
                'dimension_computation_date': dataset['dimension_computation_date']
            },
            'quality_metrics': {
                'analysis_quality_score': quality['analysis_quality_score'] if quality else None,
                'content_analyzed': quality['content_analyzed'] if quality else False,
                'schema_columns': json.loads(quality['schema_columns']) if quality and quality['schema_columns'] else [],
                'schema_dtypes': json.loads(quality['schema_dtypes']) if quality and quality['schema_dtypes'] else {}
            },
            'history': [
                {
                    'snapshot_date': row['snapshot_date'],
                    'row_count': row['row_count'],
                    'column_count': row['column_count'],
                    'file_size': row['file_size'],
                    'status_code': row['status_code'],
                    'availability': row['availability'],
                    'last_modified': row['last_modified']
                } for row in history
            ],
            'changes': [
                {
                    'change_type': row['change_type'],
                    'change_description': row['change_description'],
                    'change_date': row['change_date'],
                    'old_value': row['old_value'],
                    'new_value': row['new_value']
                } for row in changes
            ],
            'agency_info': agency_info,
            'metadata': {
                'first_published': min([row['snapshot_date'] for row in history]) if history else None,
                'last_updated': dataset['snapshot_date'],
                'total_snapshots': len(history),
                'formats_available': [dataset['resource_format']] if dataset['resource_format'] else [],
                'unique_identifier': dataset_id
            }
        }
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@dataset_detail_bp.route('/api/dataset/<dataset_id>/history')
def get_dataset_history(dataset_id: str):
    """Get detailed dataset history"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT snapshot_date, row_count, column_count, file_size, status_code,
                   content_hash, schema_hash, availability, last_modified, 
                   analysis_quality_score, content_analyzed
            FROM dataset_states
            WHERE dataset_id = ?
            ORDER BY snapshot_date DESC
        """, (dataset_id,))
        
        history = cursor.fetchall()
        conn.close()
        
        return jsonify([
            {
                'snapshot_date': row['snapshot_date'],
                'row_count': row['row_count'],
                'column_count': row['column_count'],
                'file_size': row['file_size'],
                'status_code': row['status_code'],
                'availability': row['availability'],
                'last_modified': row['last_modified'],
                'quality_score': row['analysis_quality_score'],
                'content_analyzed': bool(row['content_analyzed'])
            } for row in history
        ])
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@dataset_detail_bp.route('/api/dataset/<dataset_id>/changes')
def get_dataset_changes(dataset_id: str):
    """Get dataset change events"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT change_type, change_description, change_date, old_value, new_value,
                   change_impact, change_severity
            FROM dataset_changes
            WHERE dataset_id = ?
            ORDER BY change_date DESC
        """, (dataset_id,))
        
        changes = cursor.fetchall()
        conn.close()
        
        return jsonify([
            {
                'change_type': row['change_type'],
                'change_description': row['change_description'],
                'change_date': row['change_date'],
                'old_value': row['old_value'],
                'new_value': row['new_value'],
                'change_impact': row['change_impact'],
                'change_severity': row['change_severity']
            } for row in changes
        ])
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@dataset_detail_bp.route('/api/dataset/<dataset_id>/quality')
def get_dataset_quality(dataset_id: str):
    """Get dataset quality metrics"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT analysis_quality_score, content_analyzed, schema_columns, schema_dtypes,
                   dimension_computation_date, dimension_computation_time_ms
            FROM dataset_states
            WHERE dataset_id = ? AND analysis_quality_score IS NOT NULL
            ORDER BY snapshot_date DESC
            LIMIT 1
        """, (dataset_id,))
        
        quality = cursor.fetchone()
        conn.close()
        
        if not quality:
            return jsonify({'error': 'No quality data available'}), 404
        
        return jsonify({
            'quality_score': quality['analysis_quality_score'],
            'content_analyzed': bool(quality['content_analyzed']),
            'schema_columns': json.loads(quality['schema_columns']) if quality['schema_columns'] else [],
            'schema_dtypes': json.loads(quality['schema_dtypes']) if quality['schema_dtypes'] else {},
            'computation_date': quality['dimension_computation_date'],
            'computation_time_ms': quality['dimension_computation_time_ms']
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
