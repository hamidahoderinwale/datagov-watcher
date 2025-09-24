"""
Export API - Data export functionality for various formats
"""

from flask import Blueprint, jsonify, request, send_file
import sqlite3
import json
import csv
import io
from datetime import datetime, timedelta
import pandas as pd

export_bp = Blueprint('export', __name__, url_prefix='/api/export')

def get_db_connection():
    """Get database connection"""
    return sqlite3.connect('datasets.db')

@export_bp.route('/analytics')
def export_analytics():
    """Export analytics data in various formats"""
    format_type = request.args.get('format', 'csv')
    time_period = int(request.args.get('time_period', 30))
    metric = request.args.get('metric', 'datasets')
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=time_period)
        
        # Get analytics data based on metric
        if metric == 'datasets':
            data = get_dataset_analytics(cursor, start_date, end_date)
        elif metric == 'availability':
            data = get_availability_analytics(cursor, start_date, end_date)
        elif metric == 'changes':
            data = get_changes_analytics(cursor, start_date, end_date)
        elif metric == 'snapshots':
            data = get_snapshots_analytics(cursor, start_date, end_date)
        else:
            data = get_dataset_analytics(cursor, start_date, end_date)
        
        conn.close()
        
        # Export based on format
        if format_type == 'csv':
            return export_csv(data, f'analytics_{metric}_{time_period}d.csv')
        elif format_type == 'json':
            return export_json(data, f'analytics_{metric}_{time_period}d.json')
        elif format_type == 'excel':
            return export_excel(data, f'analytics_{metric}_{time_period}d.xlsx')
        else:
            return jsonify({'error': 'Unsupported format'}), 400
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def get_dataset_analytics(cursor, start_date, end_date):
    """Get dataset analytics data"""
    cursor.execute("""
        SELECT 
            DATE(snapshot_date) as date,
            COUNT(DISTINCT dataset_id) as total_datasets,
            COUNT(CASE WHEN availability = 'available' THEN 1 END) as available_datasets,
            COUNT(CASE WHEN availability = 'unavailable' THEN 1 END) as unavailable_datasets
        FROM dataset_states 
        WHERE snapshot_date >= ? AND snapshot_date <= ?
        GROUP BY DATE(snapshot_date)
        ORDER BY date
    """, (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
    
    return cursor.fetchall()

def get_availability_analytics(cursor, start_date, end_date):
    """Get availability analytics data"""
    cursor.execute("""
        SELECT 
            DATE(snapshot_date) as date,
            availability,
            COUNT(*) as count
        FROM dataset_states 
        WHERE snapshot_date >= ? AND snapshot_date <= ?
        GROUP BY DATE(snapshot_date), availability
        ORDER BY date, availability
    """, (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
    
    return cursor.fetchall()

def get_changes_analytics(cursor, start_date, end_date):
    """Get changes analytics data"""
    cursor.execute("""
        SELECT 
            DATE(snapshot_date) as date,
            COUNT(*) as change_events
        FROM dataset_states ds1
        WHERE ds1.snapshot_date >= ? AND ds1.snapshot_date <= ?
        AND EXISTS (
            SELECT 1 FROM dataset_states ds2 
            WHERE ds2.dataset_id = ds1.dataset_id 
            AND ds2.snapshot_date < ds1.snapshot_date
            AND ds2.content_hash != ds1.content_hash
        )
        GROUP BY DATE(snapshot_date)
        ORDER BY date
    """, (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
    
    return cursor.fetchall()

def get_snapshots_analytics(cursor, start_date, end_date):
    """Get snapshots analytics data"""
    cursor.execute("""
        SELECT 
            DATE(snapshot_date) as date,
            COUNT(*) as total_snapshots,
            COUNT(DISTINCT dataset_id) as unique_datasets
        FROM dataset_states 
        WHERE snapshot_date >= ? AND snapshot_date <= ?
        GROUP BY DATE(snapshot_date)
        ORDER BY date
    """, (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
    
    return cursor.fetchall()

def export_csv(data, filename):
    """Export data as CSV"""
    output = io.StringIO()
    writer = csv.writer(output)
    
    if data:
        # Write headers
        if len(data[0]) == 4:  # dataset analytics
            writer.writerow(['Date', 'Total Datasets', 'Available Datasets', 'Unavailable Datasets'])
        elif len(data[0]) == 3:  # availability analytics
            writer.writerow(['Date', 'Availability', 'Count'])
        elif len(data[0]) == 2:  # changes/snapshots analytics
            writer.writerow(['Date', 'Count'])
        
        # Write data
        for row in data:
            writer.writerow(row)
    
    output.seek(0)
    
    return send_file(
        io.BytesIO(output.getvalue().encode('utf-8')),
        mimetype='text/csv',
        as_attachment=True,
        download_name=filename
    )

def export_json(data, filename):
    """Export data as JSON"""
    result = {
        'export_date': datetime.now().isoformat(),
        'data': data,
        'total_records': len(data)
    }
    
    output = io.BytesIO()
    output.write(json.dumps(result, indent=2).encode('utf-8'))
    output.seek(0)
    
    return send_file(
        output,
        mimetype='application/json',
        as_attachment=True,
        download_name=filename
    )

def export_excel(data, filename):
    """Export data as Excel"""
    output = io.BytesIO()
    
    if data:
        # Create DataFrame
        if len(data[0]) == 4:  # dataset analytics
            df = pd.DataFrame(data, columns=['Date', 'Total Datasets', 'Available Datasets', 'Unavailable Datasets'])
        elif len(data[0]) == 3:  # availability analytics
            df = pd.DataFrame(data, columns=['Date', 'Availability', 'Count'])
        elif len(data[0]) == 2:  # changes/snapshots analytics
            df = pd.DataFrame(data, columns=['Date', 'Count'])
        else:
            df = pd.DataFrame(data)
        
        # Write to Excel
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Analytics', index=False)
    
    output.seek(0)
    
    return send_file(
        output,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=filename
    )

@export_bp.route('/dataset/<dataset_id>')
def export_dataset(dataset_id):
    """Export specific dataset data"""
    format_type = request.args.get('format', 'json')
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get dataset information
        cursor.execute("""
            SELECT 
                d.id,
                d.title,
                d.agency,
                d.url,
                d.description,
                ds.snapshot_date,
                ds.availability,
                ds.row_count,
                ds.column_count,
                ds.file_size,
                ds.content_hash,
                ds.content_type,
                ds.resource_format
            FROM datasets d
            LEFT JOIN dataset_states ds ON d.id = ds.dataset_id
            WHERE d.id = ?
            ORDER BY ds.snapshot_date DESC
        """, (dataset_id,))
        
        data = cursor.fetchall()
        conn.close()
        
        if not data:
            return jsonify({'error': 'Dataset not found'}), 404
        
        # Export based on format
        if format_type == 'csv':
            return export_dataset_csv(data, f'dataset_{dataset_id}.csv')
        elif format_type == 'json':
            return export_dataset_json(data, f'dataset_{dataset_id}.json')
        else:
            return jsonify({'error': 'Unsupported format'}), 400
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def export_dataset_csv(data, filename):
    """Export dataset data as CSV"""
    output = io.StringIO()
    writer = csv.writer(output)
    
    # Write headers
    writer.writerow([
        'ID', 'Title', 'Agency', 'URL', 'Description',
        'Snapshot Date', 'Availability', 'Row Count', 'Column Count',
        'File Size', 'Content Hash', 'Content Type', 'Resource Format'
    ])
    
    # Write data
    for row in data:
        writer.writerow(row)
    
    output.seek(0)
    
    return send_file(
        io.BytesIO(output.getvalue().encode('utf-8')),
        mimetype='text/csv',
        as_attachment=True,
        download_name=filename
    )

def export_dataset_json(data, filename):
    """Export dataset data as JSON"""
    result = {
        'export_date': datetime.now().isoformat(),
        'dataset_id': data[0][0] if data else None,
        'snapshots': []
    }
    
    for row in data:
        result['snapshots'].append({
            'snapshot_date': row[5],
            'availability': row[6],
            'row_count': row[7],
            'column_count': row[8],
            'file_size': row[9],
            'content_hash': row[10],
            'content_type': row[11],
            'resource_format': row[12]
        })
    
    output = io.BytesIO()
    output.write(json.dumps(result, indent=2).encode('utf-8'))
    output.seek(0)
    
    return send_file(
        output,
        mimetype='application/json',
        as_attachment=True,
        download_name=filename
    )

@export_bp.route('/agencies')
def export_agencies():
    """Export agency data"""
    format_type = request.args.get('format', 'csv')
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get agency data
        cursor.execute("""
            SELECT 
                agency,
                COUNT(*) as dataset_count,
                COUNT(CASE WHEN ds.availability = 'available' THEN 1 END) as available_count,
                COUNT(CASE WHEN ds.availability = 'unavailable' THEN 1 END) as unavailable_count
            FROM datasets d
            LEFT JOIN dataset_states ds ON d.id = ds.dataset_id
            WHERE ds.snapshot_date = (
                SELECT MAX(snapshot_date) 
                FROM dataset_states ds2 
                WHERE ds2.dataset_id = d.id
            )
            GROUP BY agency
            ORDER BY dataset_count DESC
        """)
        
        data = cursor.fetchall()
        conn.close()
        
        # Export based on format
        if format_type == 'csv':
            return export_agencies_csv(data, 'agencies.csv')
        elif format_type == 'json':
            return export_agencies_json(data, 'agencies.json')
        else:
            return jsonify({'error': 'Unsupported format'}), 400
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def export_agencies_csv(data, filename):
    """Export agency data as CSV"""
    output = io.StringIO()
    writer = csv.writer(output)
    
    # Write headers
    writer.writerow(['Agency', 'Total Datasets', 'Available', 'Unavailable', 'Availability %'])
    
    # Write data
    for row in data:
        availability_pct = round((row[2] / row[1]) * 100, 2) if row[1] > 0 else 0
        writer.writerow([row[0], row[1], row[2], row[3], availability_pct])
    
    output.seek(0)
    
    return send_file(
        io.BytesIO(output.getvalue().encode('utf-8')),
        mimetype='text/csv',
        as_attachment=True,
        download_name=filename
    )

def export_agencies_json(data, filename):
    """Export agency data as JSON"""
    result = {
        'export_date': datetime.now().isoformat(),
        'agencies': []
    }
    
    for row in data:
        availability_pct = round((row[2] / row[1]) * 100, 2) if row[1] > 0 else 0
        result['agencies'].append({
            'agency': row[0],
            'total_datasets': row[1],
            'available': row[2],
            'unavailable': row[3],
            'availability_percentage': availability_pct
        })
    
    output = io.BytesIO()
    output.write(json.dumps(result, indent=2).encode('utf-8'))
    output.seek(0)
    
    return send_file(
        output,
        mimetype='application/json',
        as_attachment=True,
        download_name=filename
    )

