"""
Analytics API - Comprehensive analytics and reporting endpoints
"""

from flask import Blueprint, jsonify, request
import sqlite3
import json
from datetime import datetime, timedelta
from collections import defaultdict

analytics_bp = Blueprint('analytics', __name__, url_prefix='/api/analytics')

def get_db_connection():
    """Get database connection"""
    return sqlite3.connect('datasets.db')

@analytics_bp.route('/trends')
def get_trends():
    """Get trend data for analytics charts"""
    try:
        period = int(request.args.get('period', 30))
        metric = request.args.get('metric', 'datasets')
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=period)
        
        if metric == 'datasets':
            cursor.execute("""
                SELECT 
                    DATE(snapshot_date) as date, 
                    COUNT(DISTINCT dataset_id) as total_count,
                    COUNT(DISTINCT CASE WHEN availability = 'available' THEN dataset_id END) as available_count,
                    COUNT(DISTINCT CASE WHEN availability = 'unavailable' THEN dataset_id END) as unavailable_count
                FROM dataset_states 
                WHERE snapshot_date >= ? AND snapshot_date <= ?
                GROUP BY DATE(snapshot_date)
                ORDER BY date
            """, (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
        elif metric == 'availability':
            cursor.execute("""
                SELECT DATE(snapshot_date) as date, 
                       COUNT(CASE WHEN availability = 'available' THEN 1 END) as available,
                       COUNT(*) as total
                FROM dataset_states 
                WHERE snapshot_date >= ? AND snapshot_date <= ?
                GROUP BY DATE(snapshot_date)
                ORDER BY date
            """, (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
        elif metric == 'changes':
            cursor.execute("""
                SELECT DATE(snapshot_date) as date, COUNT(*) as count
                FROM dataset_states 
                WHERE snapshot_date >= ? AND snapshot_date <= ?
                AND content_hash IS NOT NULL
                GROUP BY DATE(snapshot_date)
                ORDER BY date
            """, (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
        elif metric == 'snapshots':
            cursor.execute("""
                SELECT DATE(snapshot_date) as date, COUNT(*) as count
                FROM dataset_states 
                WHERE snapshot_date >= ? AND snapshot_date <= ?
                GROUP BY DATE(snapshot_date)
                ORDER BY date
            """, (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
        
        data = cursor.fetchall()
        conn.close()
        
        if not data:
            return jsonify({
                'labels': [],
                'datasets': [{'data': []}]
            })
        
        labels = []
        total_values = []
        available_values = []
        unavailable_values = []
        
        for row in data:
            labels.append(row[0])
            if metric == 'datasets':
                total_values.append(row[1])
                available_values.append(row[2])
                unavailable_values.append(row[3])
            elif metric == 'availability':
                percentage = (row[1] / row[2] * 100) if row[2] > 0 else 0
                total_values.append(round(percentage, 1))
            else:
                total_values.append(row[1])
        
        # Prepare response based on metric type
        if metric == 'datasets':
            return jsonify({
                'labels': labels,
                'total': total_values,
                'available': available_values,
                'unavailable': unavailable_values,
                'datasets': [{
                    'label': 'Total Datasets',
                    'data': total_values
                }, {
                    'label': 'Available Datasets',
                    'data': available_values
                }, {
                    'label': 'Unavailable Datasets',
                    'data': unavailable_values
                }],
                'stats': {
                    'total_datasets': max(total_values) if total_values else 0,
                    'avg_available': round(sum(available_values) / len(available_values), 1) if available_values else 0,
                    'avg_unavailable': round(sum(unavailable_values) / len(unavailable_values), 1) if unavailable_values else 0
                }
            })
        else:
            return jsonify({
                'labels': labels,
                'datasets': [{
                    'label': metric.title(),
                    'data': total_values
                }]
            })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@analytics_bp.route('/status-distribution')
def get_status_distribution():
    """Get status distribution data"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT COALESCE(availability, 'unknown') as status, COUNT(*) as count
            FROM dataset_states ds
            INNER JOIN (
                SELECT dataset_id, MAX(created_at) as max_created
                FROM dataset_states 
                GROUP BY dataset_id
            ) latest ON ds.dataset_id = latest.dataset_id 
            AND ds.created_at = latest.max_created
            GROUP BY COALESCE(availability, 'unknown')
        """)
        
        distribution = dict(cursor.fetchall())
        conn.close()
        
        return jsonify({
            'distribution': distribution,
            'total': sum(distribution.values())
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@analytics_bp.route('/format-distribution')
def get_format_distribution():
    """Get format distribution data"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT COALESCE(resource_format, 'unknown') as format, COUNT(*) as count
            FROM dataset_states ds
            INNER JOIN (
                SELECT dataset_id, MAX(created_at) as max_created
                FROM dataset_states 
                GROUP BY dataset_id
            ) latest ON ds.dataset_id = latest.dataset_id 
            AND ds.created_at = latest.max_created
            GROUP BY COALESCE(resource_format, 'unknown')
            ORDER BY count DESC
        """)
        
        distribution = dict(cursor.fetchall())
        conn.close()
        
        return jsonify({
            'distribution': distribution,
            'total': sum(distribution.values())
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@analytics_bp.route('/change-frequency')
def get_change_frequency():
    """Get change frequency data"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get changes in the last 30 days
        thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        cursor.execute("""
            SELECT 
                CASE 
                    WHEN availability = 'available' THEN 'Available'
                    WHEN availability = 'unavailable' THEN 'Unavailable'
                    ELSE 'Unknown'
                END as change_type,
                COUNT(*) as count
            FROM dataset_states 
            WHERE snapshot_date >= ?
            GROUP BY availability
        """, (thirty_days_ago,))
        
        frequency = dict(cursor.fetchall())
        
        # Get total changes
        cursor.execute("""
            SELECT COUNT(*) 
            FROM dataset_states 
            WHERE snapshot_date >= ?
        """, (thirty_days_ago,))
        
        total_changes = cursor.fetchone()[0]
        conn.close()
        
        return jsonify({
            'frequency': frequency,
            'total_changes': total_changes
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@analytics_bp.route('/quality-metrics')
def get_quality_metrics():
    """Get data quality metrics"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get quality metrics
        cursor.execute("""
            SELECT 
                COUNT(*) as total_datasets,
                COUNT(CASE WHEN availability = 'available' THEN 1 END) as available_datasets,
                COUNT(CASE WHEN row_count IS NOT NULL THEN 1 END) as datasets_with_row_count,
                COUNT(CASE WHEN column_count IS NOT NULL THEN 1 END) as datasets_with_column_count,
                COUNT(CASE WHEN content_hash IS NOT NULL THEN 1 END) as datasets_with_content_hash
            FROM dataset_states ds
            INNER JOIN (
                SELECT dataset_id, MAX(created_at) as max_created
                FROM dataset_states 
                GROUP BY dataset_id
            ) latest ON ds.dataset_id = latest.dataset_id 
            AND ds.created_at = latest.max_created
        """)
        
        row = cursor.fetchone()
        total = row[0]
        
        metrics = {
            'availability_rate': round((row[1] / total * 100) if total > 0 else 0, 1),
            'row_count_completeness': round((row[2] / total * 100) if total > 0 else 0, 1),
            'column_count_completeness': round((row[3] / total * 100) if total > 0 else 0, 1),
            'content_hash_completeness': round((row[4] / total * 100) if total > 0 else 0, 1)
        }
        
        conn.close()
        
        return jsonify({
            'metrics': metrics,
            'total_datasets': total
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@analytics_bp.route('/stats')
def get_analytics_stats():
    """Get comprehensive analytics statistics"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get basic counts
        cursor.execute("SELECT COUNT(DISTINCT dataset_id) FROM dataset_states")
        total_datasets = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM dataset_states")
        total_snapshots = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT agency) FROM dataset_states WHERE agency IS NOT NULL")
        total_agencies = cursor.fetchone()[0]
        
        # Get availability stats
        cursor.execute("""
            SELECT 
                COUNT(CASE WHEN availability = 'available' THEN 1 END) as available,
                COUNT(CASE WHEN availability = 'unavailable' THEN 1 END) as unavailable,
                COUNT(CASE WHEN availability IS NULL THEN 1 END) as unknown
            FROM dataset_states ds
            INNER JOIN (
                SELECT dataset_id, MAX(created_at) as max_created
                FROM dataset_states 
                GROUP BY dataset_id
            ) latest ON ds.dataset_id = latest.dataset_id 
            AND ds.created_at = latest.max_created
        """)
        
        availability_stats = cursor.fetchone()
        
        # Get recent activity
        cursor.execute("""
            SELECT COUNT(*) 
            FROM dataset_states 
            WHERE snapshot_date >= date('now', '-7 days')
        """)
        recent_snapshots = cursor.fetchone()[0]
        
        # Calculate changes from last week
        week_ago = datetime.now() - timedelta(days=7)
        
        # Datasets change
        cursor.execute("""
            SELECT COUNT(DISTINCT dataset_id) 
            FROM dataset_states 
            WHERE snapshot_date >= date('now', '-14 days') 
            AND snapshot_date < date('now', '-7 days')
        """)
        datasets_last_week = cursor.fetchone()[0]
        datasets_change = total_datasets - datasets_last_week if datasets_last_week > 0 else 0
        
        # Agencies change
        cursor.execute("""
            SELECT COUNT(DISTINCT agency) 
            FROM dataset_states 
            WHERE snapshot_date >= date('now', '-14 days') 
            AND snapshot_date < date('now', '-7 days')
            AND agency IS NOT NULL
        """)
        agencies_last_week = cursor.fetchone()[0]
        agencies_change = total_agencies - agencies_last_week if agencies_last_week > 0 else 0
        
        # Snapshots change
        cursor.execute("""
            SELECT COUNT(*) 
            FROM dataset_states 
            WHERE snapshot_date >= date('now', '-14 days') 
            AND snapshot_date < date('now', '-7 days')
        """)
        snapshots_last_week = cursor.fetchone()[0]
        snapshots_change = total_snapshots - snapshots_last_week if snapshots_last_week > 0 else 0
        
        # Available datasets change
        cursor.execute("""
            SELECT COUNT(CASE WHEN availability = 'available' THEN 1 END)
            FROM dataset_states ds
            INNER JOIN (
                SELECT dataset_id, MAX(created_at) as max_created
                FROM dataset_states 
                WHERE snapshot_date >= date('now', '-14 days') 
                AND snapshot_date < date('now', '-7 days')
                GROUP BY dataset_id
            ) latest ON ds.dataset_id = latest.dataset_id 
            AND ds.created_at = latest.max_created
        """)
        available_last_week = cursor.fetchone()[0]
        available_change = availability_stats[0] - available_last_week if available_last_week > 0 else 0
        
        # Changes change (content changes)
        cursor.execute("""
            SELECT COUNT(*) 
            FROM dataset_states 
            WHERE snapshot_date >= date('now', '-14 days') 
            AND snapshot_date < date('now', '-7 days')
            AND content_hash IS NOT NULL
        """)
        changes_last_week = cursor.fetchone()[0]
        changes_change = recent_snapshots - changes_last_week if changes_last_week > 0 else 0
        
        # Availability rate change
        total_last_week = datasets_last_week
        availability_rate_last_week = (available_last_week / total_last_week * 100) if total_last_week > 0 else 0
        availability_rate_current = (availability_stats[0] / total_datasets * 100) if total_datasets > 0 else 0
        availability_change = round(availability_rate_current - availability_rate_last_week, 1)
        
        conn.close()
        
        return jsonify({
            'total_datasets': total_datasets,
            'total_snapshots': total_snapshots,
            'total_agencies': total_agencies,
            'availability_stats': {
                'available': availability_stats[0],
                'unavailable': availability_stats[1],
                'unknown': availability_stats[2]
            },
            'recent_snapshots': recent_snapshots,
            'last_updated': datetime.now().isoformat(),
            # Change indicators
            'datasets_change': datasets_change,
            'agencies_change': agencies_change,
            'snapshots_change': snapshots_change,
            'available_change': available_change,
            'changes_change': changes_change,
            'availability_change': availability_change
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@analytics_bp.route('/agencies')
def get_agencies():
    """Get agency distribution data for analytics charts"""
    try:
        filter_type = request.args.get('filter', 'all')
        limit = int(request.args.get('limit', 20))
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Build query based on filter
        if filter_type == 'top10':
            limit = 10
        elif filter_type == 'top20':
            limit = 20
        
        # Get agency distribution from latest snapshots
        cursor.execute("""
            SELECT 
                COALESCE(agency, 'Unknown') as agency,
                COUNT(*) as dataset_count,
                COUNT(CASE WHEN availability = 'available' THEN 1 END) as available_count,
                COUNT(CASE WHEN availability = 'unavailable' THEN 1 END) as unavailable_count
            FROM dataset_states ds
            INNER JOIN (
                SELECT dataset_id, MAX(created_at) as max_created
                FROM dataset_states 
                GROUP BY dataset_id
            ) latest ON ds.dataset_id = latest.dataset_id 
            AND ds.created_at = latest.max_created
            GROUP BY COALESCE(agency, 'Unknown')
            ORDER BY dataset_count DESC
            LIMIT ?
        """, (limit,))
        
        results = cursor.fetchall()
        conn.close()
        
        # Process data for chart
        agencies = []
        dataset_counts = []
        available_counts = []
        unavailable_counts = []
        
        for row in results:
            agency, dataset_count, available_count, unavailable_count = row
            agencies.append(agency)
            dataset_counts.append(dataset_count)
            available_counts.append(available_count or 0)
            unavailable_counts.append(unavailable_count or 0)
        
        # Generate colors for each agency
        colors = [
            '#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6',
            '#06b6d4', '#84cc16', '#f97316', '#ec4899', '#6366f1',
            '#14b8a6', '#f59e0b', '#ef4444', '#8b5cf6', '#06b6d4',
            '#84cc16', '#f97316', '#ec4899', '#6366f1', '#14b8a6'
        ]
        
        return jsonify({
            'labels': agencies,
            'datasets': [{
                'label': 'Total Datasets',
                'data': dataset_counts,
                'backgroundColor': colors[:len(agencies)],
                'borderColor': '#1f2937',
                'borderWidth': 1
            }, {
                'label': 'Available Datasets',
                'data': available_counts,
                'backgroundColor': colors[:len(agencies)],
                'borderColor': '#1f2937',
                'borderWidth': 1
            }],
            'summary': {
                'total_agencies': len(agencies),
                'total_datasets': sum(dataset_counts),
                'total_available': sum(available_counts),
                'total_unavailable': sum(unavailable_counts)
            }
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
