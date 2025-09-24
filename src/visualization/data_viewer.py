"""
Data Viewer for Concordance: Dataset State Historian
Shows actual data content from analyzed datasets
"""

from flask import Flask, render_template, jsonify, request
import sqlite3
import json
import pandas as pd
from datetime import datetime
from pathlib import Path
import os

app = Flask(__name__)

def get_database_connection():
    """Get database connection"""
    return sqlite3.connect("datasets.db")

def get_dataset_data(dataset_id, snapshot_date=None):
    """Get actual data content for a dataset"""
    dataset_states_dir = Path(f"dataset_states/{dataset_id}")
    
    if not dataset_states_dir.exists():
        return None
    
    # Get latest snapshot if no date specified
    if not snapshot_date:
        snapshot_dirs = [d for d in dataset_states_dir.iterdir() if d.is_dir()]
        if not snapshot_dirs:
            return None
        snapshot_date = max(snapshot_dirs, key=lambda x: x.name).name
    
    snapshot_dir = dataset_states_dir / snapshot_date
    
    # Look for data files
    data_files = []
    for file_path in snapshot_dir.iterdir():
        if file_path.is_file() and file_path.name != 'metadata.json':
            data_files.append(file_path)
    
    if not data_files:
        return {
            'dataset_id': dataset_id,
            'snapshot_date': snapshot_date,
            'data_files': [],
            'error': 'No data files found'
        }
    
    # Try to load the first data file
    data_file = data_files[0]
    file_extension = data_file.suffix.lower()
    
    try:
        if file_extension == '.csv':
            df = pd.read_csv(data_file, nrows=1000)  # Limit to first 1000 rows
            data_content = {
                'type': 'csv',
                'columns': df.columns.tolist(),
                'dtypes': df.dtypes.to_dict(),
                'shape': df.shape,
                'sample_data': df.head(10).to_dict('records'),
                'file_size': data_file.stat().st_size,
                'file_name': data_file.name
            }
        elif file_extension == '.json':
            with open(data_file, 'r') as f:
                json_data = json.load(f)
            
            if isinstance(json_data, list) and len(json_data) > 0:
                # Convert to DataFrame if it's a list of objects
                df = pd.DataFrame(json_data[:1000])  # Limit to first 1000 items
                data_content = {
                    'type': 'json',
                    'columns': df.columns.tolist(),
                    'dtypes': df.dtypes.to_dict(),
                    'shape': df.shape,
                    'sample_data': df.head(10).to_dict('records'),
                    'file_size': data_file.stat().st_size,
                    'file_name': data_file.name
                }
            else:
                data_content = {
                    'type': 'json',
                    'raw_data': json_data,
                    'file_size': data_file.stat().st_size,
                    'file_name': data_file.name
                }
        else:
            # For other file types, show basic info
            with open(data_file, 'rb') as f:
                content = f.read(1000)  # First 1000 bytes
            
            data_content = {
                'type': 'binary',
                'file_extension': file_extension,
                'file_size': data_file.stat().st_size,
                'file_name': data_file.name,
                'preview': content.decode('utf-8', errors='ignore')
            }
        
        return {
            'dataset_id': dataset_id,
            'snapshot_date': snapshot_date,
            'data_files': [f.name for f in data_files],
            'data_content': data_content
        }
        
    except Exception as e:
        return {
            'dataset_id': dataset_id,
            'snapshot_date': snapshot_date,
            'data_files': [f.name for f in data_files],
            'error': f'Error loading data: {str(e)}'
        }

def get_dataset_metadata(dataset_id):
    """Get metadata for a dataset"""
    dataset_states_dir = Path(f"dataset_states/{dataset_id}")
    
    if not dataset_states_dir.exists():
        return None
    
    # Get latest snapshot
    snapshot_dirs = [d for d in dataset_states_dir.iterdir() if d.is_dir()]
    if not snapshot_dirs:
        return None
    
    latest_snapshot = max(snapshot_dirs, key=lambda x: x.name)
    metadata_file = latest_snapshot / 'metadata.json'
    
    if metadata_file.exists():
        with open(metadata_file, 'r') as f:
            return json.load(f)
    
    return None

@app.route('/')
def index():
    """Main data viewer page"""
    return render_template('components/data_viewer.html')

@app.route('/api/dataset/<dataset_id>/data')
def api_dataset_data(dataset_id):
    """API endpoint for dataset data content"""
    snapshot_date = request.args.get('snapshot_date')
    data = get_dataset_data(dataset_id, snapshot_date)
    return jsonify(data)

@app.route('/api/dataset/<dataset_id>/metadata')
def api_dataset_metadata(dataset_id):
    """API endpoint for dataset metadata"""
    metadata = get_dataset_metadata(dataset_id)
    return jsonify(metadata)

@app.route('/api/datasets')
def api_datasets():
    """API endpoint for all datasets with basic info"""
    conn = get_database_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT ds.dataset_id, ds.snapshot_date, ds.row_count, ds.column_count, 
               ds.file_size, ds.created_at
        FROM dataset_states ds
        INNER JOIN (
            SELECT dataset_id, MAX(created_at) as max_created
            FROM dataset_states 
            GROUP BY dataset_id
        ) latest ON ds.dataset_id = latest.dataset_id 
        AND ds.created_at = latest.max_created
        ORDER BY ds.created_at DESC
    ''')
    
    columns = [description[0] for description in cursor.description]
    datasets = [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    # Add metadata
    for dataset in datasets:
        metadata = get_dataset_metadata(dataset['dataset_id'])
        if metadata:
            dataset.update({
                'title': metadata.get('title', 'Unknown'),
                'agency': metadata.get('agency', 'Unknown'),
                'url': metadata.get('url', ''),
                'availability': metadata.get('availability', 'unknown')
            })
        else:
            dataset.update({
                'title': 'Unknown',
                'agency': 'Unknown',
                'url': '',
                'availability': 'unknown'
            })
    
    conn.close()
    return jsonify(datasets)

if __name__ == '__main__':
    # Create templates directory if it doesn't exist
    os.makedirs('templates', exist_ok=True)
    
    # Run the data viewer
    app.run(debug=True, host='127.0.0.1', port=8082)
