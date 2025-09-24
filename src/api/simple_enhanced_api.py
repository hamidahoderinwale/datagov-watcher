"""
Simple Enhanced Dataset API
Provides basic enhanced dataset information
"""

from flask import Blueprint, jsonify, request
import sqlite3
import json
from datetime import datetime
from typing import Dict, List, Optional, Any

simple_enhanced_bp = Blueprint('simple_enhanced', __name__)

def get_db_connection():
    """Get database connection"""
    conn = sqlite3.connect('datasets.db')
    conn.row_factory = sqlite3.Row
    return conn

@simple_enhanced_bp.route('/api/dataset/<dataset_id>/enhanced')
def get_enhanced_dataset_info(dataset_id: str):
    """Get enhanced dataset information with additional metadata"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get latest dataset state
        cursor.execute("""
            SELECT *
            FROM dataset_states
            WHERE dataset_id = ?
            ORDER BY snapshot_date DESC
            LIMIT 1
        """, (dataset_id,))
        
        dataset = cursor.fetchone()
        if not dataset:
            conn.close()
            return jsonify({'error': 'Dataset not found'}), 404
        
        # Get historical data for analysis
        cursor.execute("""
            SELECT snapshot_date, row_count, column_count, file_size, 
                   status_code, availability, analysis_quality_score
            FROM dataset_states
            WHERE dataset_id = ?
            ORDER BY snapshot_date DESC
            LIMIT 20
        """, (dataset_id,))
        
        history = cursor.fetchall()
        
        conn.close()
        
        # Calculate basic metrics
        metrics = calculate_basic_metrics(dataset, history)
        patterns = analyze_basic_patterns(dataset, history)
        
        enhanced_info = {
            'dataset_id': dataset_id,
            'basic_info': {
                'title': dataset[12] or 'Unknown Title',  # title column
                'agency': dataset[13] or 'Unknown Agency',  # agency column
                'url': dataset[14],  # url column
                'description': dataset[15],  # description column
                'last_modified': dataset[19],  # last_modified column
                'source': 'Unknown'  # source not available in dataset_states
            },
            'technical_info': {
                'file_size': dataset[10],  # file_size column
                'content_type': dataset[16],  # content_type column
                'resource_format': dataset[17],  # resource_format column
                'status_code': dataset[15],  # status_code column
                'availability': dataset[20]  # availability column
            },
            'data_quality': {
                'dimensions_computed': bool(dataset[21]),  # dimensions_computed column
                'computation_date': dataset[22],  # dimension_computation_date column
                'computation_time_ms': dataset[24],  # dimension_computation_time_ms column
                'computation_error': dataset[23],  # dimension_computation_error column
                'content_analyzed': bool(dataset[27]),  # content_analyzed column
                'quality_score': dataset[28]  # analysis_quality_score column
            },
            'metrics': metrics,
            'data_patterns': patterns,
            'history_summary': {
                'total_snapshots': len(history),
                'date_range': {
                    'first': history[-1][0] if history else None,  # snapshot_date column (index 0)
                    'last': history[0][0] if history else None  # snapshot_date column (index 0)
                }
            }
        }
        
        return jsonify(enhanced_info)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def calculate_basic_metrics(dataset, history):
    """Calculate basic metrics for the dataset"""
    metrics = {
        'data_density': 0,
        'efficiency_score': 0,
        'accessibility_score': 0,
        'reliability_score': 0
    }
    
    # Data density (rows per MB)
    if dataset[10] and dataset[10] > 0 and dataset[8]:  # file_size and row_count columns
        metrics['data_density'] = (dataset[8] * 1024 * 1024) / dataset[10]
    
    # Efficiency score (based on file size vs data content)
    if dataset[10] and dataset[8]:  # file_size and row_count columns
        expected_size = dataset[8] * dataset[9] * 10  # Rough estimate (row_count * column_count)
        if expected_size > 0:
            efficiency = min(100, (expected_size / dataset[10]) * 100)
            metrics['efficiency_score'] = efficiency
    
    # Accessibility score (based on status code and availability)
    if dataset[15] == 200 and dataset[20] == 'available':  # status_code and availability columns
        metrics['accessibility_score'] = 100
    elif dataset[15] and 200 <= dataset[15] < 300:  # status_code column
        metrics['accessibility_score'] = 80
    elif dataset[15] and 300 <= dataset[15] < 400:  # status_code column
        metrics['accessibility_score'] = 60
    else:
        metrics['accessibility_score'] = 0
    
    # Reliability score (based on quality metrics)
    if dataset[28]:  # analysis_quality_score column
        metrics['reliability_score'] = dataset[28]
    elif dataset[27]:  # content_analyzed column
        metrics['reliability_score'] = 70  # Analyzed but no specific score
    else:
        metrics['reliability_score'] = 30  # Not analyzed
    
    return metrics

def analyze_basic_patterns(dataset, history):
    """Analyze basic data patterns"""
    patterns = {
        'data_volume': categorize_data_volume(dataset[10]),  # file_size column
        'format_category': categorize_format(dataset[17]),  # resource_format column
        'update_frequency': analyze_update_frequency(history),
        'stability_score': calculate_stability_score(history),
        'complexity_score': calculate_complexity_score(dataset)
    }
    return patterns

def categorize_data_volume(file_size):
    """Categorize data volume based on file size"""
    if not file_size:
        return 'Unknown'
    elif file_size < 1024:  # < 1KB
        return 'Tiny'
    elif file_size < 1024 * 1024:  # < 1MB
        return 'Small'
    elif file_size < 10 * 1024 * 1024:  # < 10MB
        return 'Medium'
    elif file_size < 100 * 1024 * 1024:  # < 100MB
        return 'Large'
    else:
        return 'Very Large'

def categorize_format(format_str):
    """Categorize data format"""
    if not format_str:
        return 'Unknown'
    
    format_lower = format_str.lower()
    
    if any(fmt in format_lower for fmt in ['csv', 'tsv', 'txt']):
        return 'Tabular'
    elif any(fmt in format_lower for fmt in ['json', 'geojson']):
        return 'Structured'
    elif any(fmt in format_lower for fmt in ['xml', 'rdf']):
        return 'Markup'
    elif any(fmt in format_lower for fmt in ['xls', 'xlsx', 'ods']):
        return 'Spreadsheet'
    elif any(fmt in format_lower for fmt in ['pdf', 'doc', 'docx']):
        return 'Document'
    elif any(fmt in format_lower for fmt in ['api', 'rest', 'graphql']):
        return 'API'
    elif any(fmt in format_lower for fmt in ['html', 'htm']):
        return 'Web'
    else:
        return 'Other'

def analyze_update_frequency(history):
    """Analyze how frequently the dataset is updated"""
    if len(history) <= 1:
        return 'Static'
    elif len(history) <= 5:
        return 'Rarely Updated'
    elif len(history) <= 20:
        return 'Occasionally Updated'
    elif len(history) <= 100:
        return 'Frequently Updated'
    else:
        return 'Very Active'

def calculate_stability_score(history):
    """Calculate dataset stability score (0-100)"""
    if len(history) < 2:
        return 100.0  # Single snapshot is considered stable
    
    # Calculate stability based on content changes
    content_changes = 0
    for i in range(1, len(history)):
        if history[i-1][1] != history[i][1]:  # row_count column (index 1)
            content_changes += 1
    
    total_snapshots = len(history) - 1
    stability = 100 - (content_changes / total_snapshots) * 50
    
    return max(0, min(100, stability))

def calculate_complexity_score(dataset):
    """Calculate dataset complexity score (0-100)"""
    score = 0
    
    # File size complexity
    if dataset[10]:  # file_size column
        if dataset[10] > 100 * 1024 * 1024:  # > 100MB
            score += 20
        elif dataset[10] > 10 * 1024 * 1024:  # > 10MB
            score += 15
        elif dataset[10] > 1024 * 1024:  # > 1MB
            score += 10
    
    # Format complexity
    if dataset[17]:  # resource_format column
        format_str = dataset[17].lower()
        if any(fmt in format_str for fmt in ['api', 'rest', 'graphql']):
            score += 25
        elif any(fmt in format_str for fmt in ['xml', 'rdf', 'json']):
            score += 15
        elif any(fmt in format_str for fmt in ['csv', 'tsv']):
            score += 5
    
    # Data volume complexity
    if dataset[8] and dataset[8] > 0:  # row_count column
        if dataset[8] > 1000000:  # > 1M rows
            score += 25
        elif dataset[8] > 100000:  # > 100K rows
            score += 15
        elif dataset[8] > 10000:  # > 10K rows
            score += 10
    
    return min(100, score)
