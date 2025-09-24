"""
Metadata API - Comprehensive dataset metadata and schema information
"""

from flask import Blueprint, jsonify, request
import sqlite3
import json
from datetime import datetime, timedelta
import requests
import re

metadata_bp = Blueprint('metadata', __name__, url_prefix='/api/metadata')

def get_db_connection():
    """Get database connection"""
    return sqlite3.connect('datasets.db')

@metadata_bp.route('/dataset/<dataset_id>')
def get_dataset_metadata(dataset_id):
    """Get comprehensive metadata for a specific dataset"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get basic dataset information
        cursor.execute("""
            SELECT 
                id,
                title,
                agency,
                url,
                description,
                last_modified,
                source,
                created_at
            FROM datasets 
            WHERE id = ?
        """, (dataset_id,))
        
        dataset = cursor.fetchone()
        if not dataset:
            return jsonify({'error': 'Dataset not found'}), 404
        
        # Get latest state information
        cursor.execute("""
            SELECT 
                snapshot_date,
                availability,
                row_count,
                column_count,
                file_size,
                content_hash,
                content_type,
                resource_format,
                schema,
                schema_columns,
                schema_dtypes,
                last_modified,
                dimensions_computed,
                dimension_computation_date,
                analysis_quality_score
            FROM dataset_states 
            WHERE dataset_id = ? 
            ORDER BY snapshot_date DESC 
            LIMIT 1
        """, (dataset_id,))
        
        latest_state = cursor.fetchone()
        
        # Get all historical states for timeline
        cursor.execute("""
            SELECT 
                snapshot_date,
                availability,
                row_count,
                column_count,
                file_size,
                content_hash,
                content_type,
                resource_format,
                last_modified
            FROM dataset_states 
            WHERE dataset_id = ? 
            ORDER BY snapshot_date ASC
        """, (dataset_id,))
        
        historical_states = cursor.fetchall()
        
        # Get publication and update dates
        first_published = historical_states[0][0] if historical_states else None
        last_updated = latest_state[0] if latest_state else None
        
        # Get all available formats
        cursor.execute("""
            SELECT DISTINCT resource_format, content_type
            FROM dataset_states 
            WHERE dataset_id = ? 
            AND resource_format IS NOT NULL
        """, (dataset_id,))
        
        formats = []
        for row in cursor.fetchall():
            if row[0] and row[0] not in [f['format'] for f in formats]:
                formats.append({
                    'format': row[0],
                    'content_type': row[1],
                    'display_name': get_format_display_name(row[0])
                })
        
        # Get schema information
        schema_info = None
        if latest_state and latest_state[8]:  # schema column
            try:
                schema_info = json.loads(latest_state[8])
            except:
                schema_info = None
        
        schema_columns = None
        if latest_state and latest_state[9]:  # schema_columns
            try:
                schema_columns = json.loads(latest_state[9])
            except:
                schema_columns = None
        
        schema_dtypes = None
        if latest_state and latest_state[10]:  # schema_dtypes
            try:
                schema_dtypes = json.loads(latest_state[10])
            except:
                schema_dtypes = None
        
        # Get change history
        change_history = []
        for i, state in enumerate(historical_states):
            if i > 0:
                prev_state = historical_states[i-1]
                changes = detect_changes(prev_state, state)
                if changes:
                    change_history.append({
                        'date': state[0],
                        'changes': changes
                    })
        
        # Try to get additional metadata from the dataset URL
        additional_metadata = get_additional_metadata(dataset[3])  # URL
        
        conn.close()
        
        # Build comprehensive metadata response
        metadata = {
            'basic_info': {
                'id': dataset[0],
                'title': dataset[1],
                'agency': dataset[2],
                'url': dataset[3],
                'description': dataset[4],
                'source': dataset[5],
                'created_at': dataset[6]
            },
            'publication_info': {
                'first_published': first_published,
                'last_updated': last_updated,
                'metadata_created': dataset[6],
                'metadata_updated': last_updated
            },
            'current_state': {
                'status': latest_state[1] if latest_state else 'unknown',
                'row_count': latest_state[2] if latest_state else None,
                'column_count': latest_state[3] if latest_state else None,
                'file_size': latest_state[4] if latest_state else None,
                'content_hash': latest_state[5] if latest_state else None,
                'content_type': latest_state[6] if latest_state else None,
                'resource_format': latest_state[7] if latest_state else None,
                'last_modified': latest_state[11] if latest_state else None,
                'dimensions_computed': latest_state[12] if latest_state else False,
                'analysis_quality_score': latest_state[14] if latest_state else None
            },
            'formats': formats,
            'schema': {
                'schema_info': schema_info,
                'columns': schema_columns,
                'dtypes': schema_dtypes,
                'column_count': len(schema_columns) if schema_columns else 0
            },
            'change_history': change_history,
            'additional_metadata': additional_metadata,
            'statistics': {
                'total_snapshots': len(historical_states),
                'change_events': len(change_history),
                'availability_percentage': calculate_availability_percentage(historical_states)
            }
        }
        
        return jsonify(metadata)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def get_format_display_name(format_name):
    """Get display name for format"""
    format_map = {
        'CSV': 'Comma Separated Values File',
        'JSON': 'JSON File',
        'XML': 'XML File',
        'RDF': 'RDF File',
        'XLSX': 'Excel File',
        'XLS': 'Excel File',
        'PDF': 'PDF File',
        'ZIP': 'ZIP Archive',
        'SHP': 'Shapefile',
        'KML': 'KML File',
        'GEOJSON': 'GeoJSON File'
    }
    return format_map.get(format_name.upper(), format_name)

def detect_changes(prev_state, current_state):
    """Detect changes between two states"""
    changes = []
    
    if prev_state[1] != current_state[1]:  # availability
        changes.append(f"Status changed from {prev_state[1]} to {current_state[1]}")
    
    if prev_state[2] != current_state[2]:  # row_count
        changes.append(f"Row count changed from {prev_state[2]} to {current_state[2]}")
    
    if prev_state[3] != current_state[3]:  # column_count
        changes.append(f"Column count changed from {prev_state[3]} to {current_state[3]}")
    
    if prev_state[4] != current_state[4]:  # file_size
        changes.append(f"File size changed from {prev_state[4]} to {current_state[4]}")
    
    if prev_state[5] != current_state[5]:  # content_hash
        changes.append("Content changed (hash different)")
    
    if prev_state[6] != current_state[6]:  # content_type
        changes.append(f"Content type changed from {prev_state[6]} to {current_state[6]}")
    
    if prev_state[7] != current_state[7]:  # resource_format
        changes.append(f"Resource format changed from {prev_state[7]} to {current_state[7]}")
    
    return changes

def calculate_availability_percentage(states):
    """Calculate percentage of time dataset was available"""
    if not states:
        return 0
    
    available_count = sum(1 for state in states if state[1] == 'available')
    return round((available_count / len(states)) * 100, 2)

def get_additional_metadata(url):
    """Try to get additional metadata from the dataset URL"""
    try:
        # This would typically involve making a request to the dataset URL
        # and parsing metadata, but for now we'll return a placeholder
        return {
            'license': 'Unknown',
            'publisher': 'Data.gov',
            'maintainer': 'Unknown',
            'topics': [],
            'tags': [],
            'access_level': 'Public',
            'rights': 'No license information provided'
        }
    except:
        return {
            'license': 'Unknown',
            'publisher': 'Data.gov',
            'maintainer': 'Unknown',
            'topics': [],
            'tags': [],
            'access_level': 'Public',
            'rights': 'No license information provided'
        }

@metadata_bp.route('/dataset/<dataset_id>/schema')
def get_dataset_schema(dataset_id):
    """Get detailed schema information for a dataset"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get latest schema information
        cursor.execute("""
            SELECT 
                schema,
                schema_columns,
                schema_dtypes,
                column_count,
                row_count
            FROM dataset_states 
            WHERE dataset_id = ? 
            ORDER BY snapshot_date DESC 
            LIMIT 1
        """, (dataset_id,))
        
        schema_data = cursor.fetchone()
        if not schema_data:
            return jsonify({'error': 'Schema not found'}), 404
        
        schema_info = None
        if schema_data[0]:
            try:
                schema_info = json.loads(schema_data[0])
            except:
                pass
        
        columns = None
        if schema_data[1]:
            try:
                columns = json.loads(schema_data[1])
            except:
                pass
        
        dtypes = None
        if schema_data[2]:
            try:
                dtypes = json.loads(schema_data[2])
            except:
                pass
        
        conn.close()
        
        return jsonify({
            'dataset_id': dataset_id,
            'schema_info': schema_info,
            'columns': columns,
            'dtypes': dtypes,
            'column_count': schema_data[3],
            'row_count': schema_data[4],
            'last_updated': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@metadata_bp.route('/dataset/<dataset_id>/formats')
def get_dataset_formats(dataset_id):
    """Get all available formats for a dataset"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                resource_format,
                content_type,
                file_size,
                snapshot_date,
                availability
            FROM dataset_states 
            WHERE dataset_id = ? 
            AND resource_format IS NOT NULL
            ORDER BY snapshot_date DESC
        """, (dataset_id,))
        
        formats = []
        seen_formats = set()
        
        for row in cursor.fetchall():
            format_name = row[0]
            if format_name and format_name not in seen_formats:
                seen_formats.add(format_name)
                formats.append({
                    'format': format_name,
                    'content_type': row[1],
                    'display_name': get_format_display_name(format_name),
                    'file_size': row[2],
                    'last_seen': row[3],
                    'availability': row[4]
                })
        
        conn.close()
        
        return jsonify({
            'dataset_id': dataset_id,
            'formats': formats,
            'total_formats': len(formats)
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@metadata_bp.route('/dataset/<dataset_id>/timeline')
def get_dataset_timeline(dataset_id):
    """Get detailed timeline of changes for a dataset"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                snapshot_date,
                availability,
                row_count,
                column_count,
                file_size,
                content_hash,
                content_type,
                resource_format,
                last_modified
            FROM dataset_states 
            WHERE dataset_id = ? 
            ORDER BY snapshot_date ASC
        """, (dataset_id,))
        
        timeline = []
        prev_state = None
        
        for row in cursor.fetchall():
            current_state = {
                'date': row[0],
                'availability': row[1],
                'row_count': row[2],
                'column_count': row[3],
                'file_size': row[4],
                'content_hash': row[5],
                'content_type': row[6],
                'resource_format': row[7],
                'last_modified': row[8]
            }
            
            changes = []
            if prev_state:
                changes = detect_changes(prev_state, current_state)
            
            timeline.append({
                **current_state,
                'changes': changes,
                'has_changes': len(changes) > 0
            })
            
            prev_state = current_state
        
        conn.close()
        
        return jsonify({
            'dataset_id': dataset_id,
            'timeline': timeline,
            'total_events': len(timeline),
            'change_events': sum(1 for item in timeline if item['has_changes'])
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

