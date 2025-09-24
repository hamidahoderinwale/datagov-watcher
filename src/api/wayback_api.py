"""
Wayback API - Historical dataset changes and versioning
"""

from flask import Blueprint, jsonify, request
import sqlite3
import json
from datetime import datetime, timedelta
from pathlib import Path
import hashlib

wayback_bp = Blueprint('wayback', __name__, url_prefix='/api/wayback')

def get_db_connection():
    """Get database connection"""
    return sqlite3.connect('datasets.db')

@wayback_bp.route('/snapshots/<dataset_id>')
def get_dataset_snapshots(dataset_id):
    """Get all snapshots for a specific dataset"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get all snapshots for the dataset
        cursor.execute("""
            SELECT 
                snapshot_date,
                availability,
                row_count,
                column_count,
                file_size,
                content_hash,
                title,
                agency,
                url
            FROM dataset_states 
            WHERE dataset_id = ? 
            ORDER BY snapshot_date DESC
        """, (dataset_id,))
        
        snapshots = []
        for row in cursor.fetchall():
            snapshots.append({
                'date': row[0],
                'status': row[1],
                'row_count': row[2],
                'column_count': row[3],
                'file_size': row[4],
                'checksum': row[5],
                'title': row[6],
                'agency': row[7],
                'url': row[8]
            })
        
        conn.close()
        
        return jsonify({
            'dataset_id': dataset_id,
            'snapshots': snapshots,
            'total_snapshots': len(snapshots)
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@wayback_bp.route('/compare/<dataset_id>/<from_date>/<to_date>')
def compare_snapshots(dataset_id, from_date, to_date):
    """Compare two snapshots of a dataset"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # First, check if the dataset exists at all
        cursor.execute("SELECT COUNT(*) FROM dataset_states WHERE dataset_id = ?", (dataset_id,))
        dataset_exists = cursor.fetchone()[0] > 0
        
        if not dataset_exists:
            return jsonify({
                'error': 'Dataset not found',
                'dataset_id': dataset_id,
                'message': 'No snapshots found for this dataset'
            }), 404
        
        # Get the first snapshot on or after from_date
        cursor.execute("""
            SELECT 
                snapshot_date,
                availability,
                row_count,
                column_count,
                file_size,
                content_hash,
                title,
                agency,
                url
            FROM dataset_states 
            WHERE dataset_id = ? AND snapshot_date >= ?
            ORDER BY snapshot_date ASC
            LIMIT 1
        """, (dataset_id, from_date))
        
        from_snapshot = cursor.fetchone()
        
        # If no snapshot found for from_date, try to get the earliest available snapshot
        if not from_snapshot:
            cursor.execute("""
                SELECT 
                    snapshot_date,
                    availability,
                    row_count,
                    column_count,
                    file_size,
                    content_hash,
                    title,
                    agency,
                    url
                FROM dataset_states 
                WHERE dataset_id = ?
                ORDER BY snapshot_date ASC
                LIMIT 1
            """, (dataset_id,))
            from_snapshot = cursor.fetchone()
        
        if not from_snapshot:
            return jsonify({
                'error': 'No snapshots found for dataset',
                'dataset_id': dataset_id,
                'message': 'No comparison data found for the selected dates'
            }), 404
        
        # Get the last snapshot on or before to_date
        cursor.execute("""
            SELECT 
                snapshot_date,
                availability,
                row_count,
                column_count,
                file_size,
                content_hash,
                title,
                agency,
                url
            FROM dataset_states 
            WHERE dataset_id = ? AND snapshot_date <= ?
            ORDER BY snapshot_date DESC
            LIMIT 1
        """, (dataset_id, to_date))
        
        to_snapshot = cursor.fetchone()
        
        # If no snapshot found for to_date, try to get the latest available snapshot
        if not to_snapshot:
            cursor.execute("""
                SELECT 
                    snapshot_date,
                    availability,
                    row_count,
                    column_count,
                    file_size,
                    content_hash,
                    title,
                    agency,
                    url
                FROM dataset_states 
                WHERE dataset_id = ?
                ORDER BY snapshot_date DESC
                LIMIT 1
            """, (dataset_id,))
            to_snapshot = cursor.fetchone()
        
        if not to_snapshot:
            return jsonify({
                'error': 'No snapshots found for dataset',
                'dataset_id': dataset_id,
                'message': 'No comparison data found for the selected dates'
            }), 404
        
        # Convert to list for consistency
        snapshots = [from_snapshot, to_snapshot]
        
        from_snapshot = {
            'date': snapshots[0][0],
            'status': snapshots[0][1],
            'row_count': snapshots[0][2],
            'column_count': snapshots[0][3],
            'file_size': snapshots[0][4],
            'checksum': snapshots[0][5],
            'title': snapshots[0][6],
            'agency': snapshots[0][7],
            'url': snapshots[0][8]
        }
        
        to_snapshot = {
            'date': snapshots[1][0],
            'status': snapshots[1][1],
            'row_count': snapshots[1][2],
            'column_count': snapshots[1][3],
            'file_size': snapshots[1][4],
            'checksum': snapshots[1][5],
            'title': snapshots[1][6],
            'agency': snapshots[1][7],
            'url': snapshots[1][8]
        }
        
        # Calculate differences
        differences = {
            'status_changed': (from_snapshot['status'] or '') != (to_snapshot['status'] or ''),
            'row_count_change': to_snapshot['row_count'] - from_snapshot['row_count'] if (from_snapshot['row_count'] is not None and to_snapshot['row_count'] is not None) else None,
            'column_count_change': to_snapshot['column_count'] - from_snapshot['column_count'] if (from_snapshot['column_count'] is not None and to_snapshot['column_count'] is not None) else None,
            'file_size_change': to_snapshot['file_size'] - from_snapshot['file_size'] if (from_snapshot['file_size'] is not None and to_snapshot['file_size'] is not None) else None,
            'content_changed': (from_snapshot['checksum'] or '') != (to_snapshot['checksum'] or ''),
            'title_changed': (from_snapshot['title'] or '') != (to_snapshot['title'] or ''),
            'agency_changed': (from_snapshot['agency'] or '') != (to_snapshot['agency'] or ''),
            'url_changed': (from_snapshot['url'] or '') != (to_snapshot['url'] or '')
        }
        
        conn.close()
        
        return jsonify({
            'dataset_id': dataset_id,
            'from_snapshot': from_snapshot,
            'to_snapshot': to_snapshot,
            'differences': differences,
            'has_changes': any(differences.values())
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@wayback_bp.route('/timeline/<dataset_id>')
def get_dataset_timeline(dataset_id):
    """Get timeline of changes for a dataset"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get timeline data
        cursor.execute("""
            SELECT 
                snapshot_date,
                availability,
                row_count,
                column_count,
                file_size,
                content_hash,
                title,
                agency
            FROM dataset_states 
            WHERE dataset_id = ? 
            ORDER BY snapshot_date ASC
        """, (dataset_id,))
        
        timeline = []
        prev_snapshot = None
        
        for row in cursor.fetchall():
            current_snapshot = {
                'date': row[0],
                'status': row[1],
                'row_count': row[2],
                'column_count': row[3],
                'file_size': row[4],
                'checksum': row[5],
                'title': row[6],
                'agency': row[7]
            }
            
            # Calculate changes from previous snapshot
            changes = []
            if prev_snapshot:
                if (prev_snapshot['status'] or '') != (current_snapshot['status'] or ''):
                    changes.append(f"Status changed from {prev_snapshot['status'] or 'unknown'} to {current_snapshot['status'] or 'unknown'}")
                
                if prev_snapshot['row_count'] != current_snapshot['row_count']:
                    row_diff = current_snapshot['row_count'] - prev_snapshot['row_count'] if (current_snapshot['row_count'] is not None and prev_snapshot['row_count'] is not None) else None
                    if row_diff is not None:
                        changes.append(f"Row count changed by {row_diff:+d}")
                
                if prev_snapshot['column_count'] != current_snapshot['column_count']:
                    col_diff = current_snapshot['column_count'] - prev_snapshot['column_count'] if (current_snapshot['column_count'] is not None and prev_snapshot['column_count'] is not None) else None
                    if col_diff is not None:
                        changes.append(f"Column count changed by {col_diff:+d}")
                
                if prev_snapshot['file_size'] != current_snapshot['file_size']:
                    size_diff = current_snapshot['file_size'] - prev_snapshot['file_size'] if (current_snapshot['file_size'] is not None and prev_snapshot['file_size'] is not None) else None
                    if size_diff is not None:
                        changes.append(f"File size changed by {size_diff:+d} bytes")
                
                if (prev_snapshot['checksum'] or '') != (current_snapshot['checksum'] or ''):
                    changes.append("Content changed (hash different)")
                
                if (prev_snapshot['title'] or '') != (current_snapshot['title'] or ''):
                    changes.append("Title changed")
                
                if (prev_snapshot['agency'] or '') != (current_snapshot['agency'] or ''):
                    changes.append("Agency changed")
            
            timeline.append({
                **current_snapshot,
                'changes': changes,
                'has_changes': len(changes) > 0
            })
            
            prev_snapshot = current_snapshot
        
        conn.close()
        
        return jsonify({
            'dataset_id': dataset_id,
            'timeline': timeline,
            'total_snapshots': len(timeline),
            'change_events': sum(1 for item in timeline if item['has_changes'])
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@wayback_bp.route('/changes/recent')
def get_recent_changes():
    """Get recent changes across all datasets"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get recent changes (last 7 days)
        seven_days_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        
        cursor.execute("""
            SELECT 
                ds.dataset_id,
                ds.title,
                ds.agency,
                ds.snapshot_date,
                ds.availability,
                ds.row_count,
                ds.column_count,
                ds.file_size,
                ds.content_hash
            FROM dataset_states ds
            WHERE ds.snapshot_date >= ?
            ORDER BY ds.snapshot_date DESC
            LIMIT 100
        """, (seven_days_ago,))
        
        changes = []
        for row in cursor.fetchall():
            changes.append({
                'dataset_id': row[0],
                'title': row[1],
                'agency': row[2],
                'date': row[3],
                'status': row[4],
                'row_count': row[5],
                'column_count': row[6],
                'file_size': row[7],
                'checksum': row[8]
            })
        
        conn.close()
        
        return jsonify({
            'changes': changes,
            'total_changes': len(changes),
            'period': '7 days'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@wayback_bp.route('/stats')
def get_wayback_stats():
    """Get wayback statistics"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get total snapshots
        cursor.execute("SELECT COUNT(*) FROM dataset_states")
        total_snapshots = cursor.fetchone()[0]
        
        # Get datasets with snapshots
        cursor.execute("""
            SELECT COUNT(DISTINCT dataset_id) 
            FROM dataset_states 
            WHERE content_hash IS NOT NULL
        """)
        datasets_with_snapshots = cursor.fetchone()[0]
        
        # Get recent activity
        cursor.execute("""
            SELECT COUNT(*) 
            FROM dataset_states 
            WHERE snapshot_date >= date('now', '-7 days')
        """)
        recent_snapshots = cursor.fetchone()[0]
        
        # Get status distribution
        cursor.execute("""
            SELECT COALESCE(availability, 'unknown') as status, COUNT(*) 
            FROM dataset_states 
            GROUP BY COALESCE(availability, 'unknown')
        """)
        status_distribution = dict(cursor.fetchall())
        
        # Get change events (datasets with different content hashes)
        cursor.execute("""
            SELECT COUNT(DISTINCT dataset_id)
            FROM dataset_states ds1
            WHERE EXISTS (
                SELECT 1 FROM dataset_states ds2 
                WHERE ds2.dataset_id = ds1.dataset_id 
                AND ds2.content_hash != ds1.content_hash
                AND ds2.snapshot_date != ds1.snapshot_date
            )
        """)
        change_events = cursor.fetchone()[0]
        
        conn.close()
        
        return jsonify({
            'total_snapshots': total_snapshots,
            'datasets_with_snapshots': datasets_with_snapshots,
            'recent_snapshots': recent_snapshots,
            'change_events': change_events,
            'status_distribution': status_distribution,
            'last_updated': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500