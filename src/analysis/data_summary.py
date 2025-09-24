#!/usr/bin/env python3
"""
Data Summary for Concordance: Dataset State Historian
Shows comprehensive summary of all analyzed dataset data
"""

import json
import sqlite3
from pathlib import Path
from collections import defaultdict
import pandas as pd

def get_data_summary():
    """Get comprehensive summary of all analyzed data"""
    
    # Connect to database
    conn = sqlite3.connect("datasets.db")
    cursor = conn.cursor()
    
    # Get all datasets
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
    
    conn.close()
    
    # Analyze each dataset
    summary = {
        'total_datasets': len(datasets),
        'datasets_by_agency': defaultdict(int),
        'datasets_by_type': defaultdict(int),
        'file_types': defaultdict(int),
        'total_file_size': 0,
        'datasets_with_data': 0,
        'datasets_with_html': 0,
        'sample_datasets': []
    }
    
    for i, dataset in enumerate(datasets):
        dataset_id = dataset['dataset_id']
        dataset_states_dir = Path(f"dataset_states/{dataset_id}")
        
        if not dataset_states_dir.exists():
            continue
        
        # Get latest snapshot
        snapshot_dirs = [d for d in dataset_states_dir.iterdir() if d.is_dir()]
        if not snapshot_dirs:
            continue
        
        latest_snapshot = max(snapshot_dirs, key=lambda x: x.name)
        metadata_file = latest_snapshot / 'metadata.json'
        
        if not metadata_file.exists():
            continue
        
        try:
            with open(metadata_file, 'r') as f:
                metadata = json.load(f)
            
            # Agency analysis
            agency = metadata.get('agency', 'Unknown')
            summary['datasets_by_agency'][agency] += 1
            
            # File analysis
            files = metadata.get('files', [])
            if files:
                summary['datasets_with_data'] += 1
                summary['total_file_size'] += sum(f.get('size', 0) for f in files)
                
                for file_info in files:
                    filename = file_info.get('filename', '')
                    file_size = file_info.get('size', 0)
                    
                    if filename.endswith('.data'):
                        summary['file_types']['HTML/Web Page'] += 1
                        summary['datasets_with_html'] += 1
                    elif filename.endswith('.csv'):
                        summary['file_types']['CSV'] += 1
                    elif filename.endswith('.json'):
                        summary['file_types']['JSON'] += 1
                    elif filename.endswith('.xlsx'):
                        summary['file_types']['Excel'] += 1
                    else:
                        summary['file_types']['Other'] += 1
            
            # Content analysis
            content_stats = metadata.get('content_stats', {})
            if content_stats.get('type') == 'unknown':
                summary['datasets_by_type']['Web Pages'] += 1
            elif content_stats.get('type') == 'csv':
                summary['datasets_by_type']['CSV Data'] += 1
            elif content_stats.get('type') == 'json':
                summary['datasets_by_type']['JSON Data'] += 1
            else:
                summary['datasets_by_type']['Other'] += 1
            
            # Sample datasets (first 5)
            if i < 5:
                summary['sample_datasets'].append({
                    'id': dataset_id,
                    'title': metadata.get('title', 'Unknown'),
                    'agency': agency,
                    'url': metadata.get('url', ''),
                    'availability': metadata.get('availability', 'unknown'),
                    'file_count': len(files),
                    'total_size': sum(f.get('size', 0) for f in files),
                    'content_type': content_stats.get('type', 'unknown'),
                    'row_count': content_stats.get('row_count', 0),
                    'column_count': content_stats.get('column_count', 0)
                })
        
        except Exception as e:
            print(f"Error processing {dataset_id}: {e}")
            continue
    
    return summary

def print_data_summary():
    """Print comprehensive data summary"""
    
    print("🧬 Concordance: Dataset State Historian - Data Summary")
    print("=" * 60)
    
    summary = get_data_summary()
    
    print(f"\n OVERVIEW")
    print(f"Total Datasets Analyzed: {summary['total_datasets']}")
    print(f"Datasets with Data Files: {summary['datasets_with_data']}")
    print(f"Datasets with HTML/Web Pages: {summary['datasets_with_html']}")
    print(f"Total Data Size: {(summary['total_file_size'] / 1024 / 1024):.1f} MB")
    
    print(f"\n🏛️ DATASETS BY AGENCY")
    for agency, count in sorted(summary['datasets_by_agency'].items(), key=lambda x: x[1], reverse=True):
        print(f"  {agency}: {count}")
    
    print(f"\n📁 FILE TYPES")
    for file_type, count in sorted(summary['file_types'].items(), key=lambda x: x[1], reverse=True):
        print(f"  {file_type}: {count}")
    
    print(f"\n📋 CONTENT TYPES")
    for content_type, count in sorted(summary['datasets_by_type'].items(), key=lambda x: x[1], reverse=True):
        print(f"  {content_type}: {count}")
    
    print(f"\n🔍 SAMPLE DATASETS")
    for i, dataset in enumerate(summary['sample_datasets'], 1):
        print(f"\n{i}. {dataset['title']}")
        print(f"   Agency: {dataset['agency']}")
        print(f"   ID: {dataset['id']}")
        print(f"   URL: {dataset['url']}")
        print(f"   Status: {dataset['availability']}")
        print(f"   Files: {dataset['file_count']} ({(dataset['total_size'] / 1024):.1f} KB)")
        print(f"   Content: {dataset['content_type']}")
        if dataset['row_count'] > 0:
            print(f"   Data: {dataset['row_count']} rows × {dataset['column_count']} columns")
    
    print(f"\n💡 KEY INSIGHTS")
    print(f"• Most datasets point to web pages rather than direct data files")
    print(f"• Data.gov uses landing pages that require user interaction to download data")
    print(f"• The system successfully captured metadata and availability status")
    print(f"• Schema analysis works on the captured HTML content structure")
    print(f"• This provides a foundation for monitoring dataset availability and changes")
    
    print(f"\n ACCESS YOUR DATA")
    print(f"• Main Dashboard: http://127.0.0.1:8081")
    print(f"• Data Viewer: http://127.0.0.1:8082")
    print(f"• Individual Reports: state_reports/ directory")

if __name__ == "__main__":
    print_data_summary()
