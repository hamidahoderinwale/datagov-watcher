"""
Enhanced Dataset API
Provides comprehensive dataset information with additional metadata and analysis
"""

from flask import Blueprint, jsonify, request
import sqlite3
import json
from datetime import datetime
from typing import Dict, List, Optional, Any
import sys
import os

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from analysis.dataset_enhancer import DatasetEnhancer

enhanced_dataset_bp = Blueprint('enhanced_dataset', __name__)

def get_db_connection():
    """Get database connection"""
    conn = sqlite3.connect('datasets.db')
    conn.row_factory = sqlite3.Row
    return conn

@enhanced_dataset_bp.route('/api/dataset/<dataset_id>/enhanced')
def get_enhanced_dataset_info(dataset_id: str):
    """Get enhanced dataset information with additional metadata"""
    try:
        enhancer = DatasetEnhancer()
        enhanced_info = enhancer.enhance_dataset_info(dataset_id)
        
        if not enhanced_info:
            return jsonify({'error': 'Dataset not found'}), 404
        
        return jsonify(enhanced_info)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@enhanced_dataset_bp.route('/api/dataset/<dataset_id>/analytics')
def get_dataset_analytics(dataset_id: str):
    """Get detailed analytics for a specific dataset"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get historical data for analytics
        cursor.execute("""
            SELECT snapshot_date, row_count, column_count, file_size, 
                   status_code, availability, analysis_quality_score,
                   content_analyzed, dimension_computation_time_ms
            FROM dataset_states
            WHERE dataset_id = ?
            ORDER BY snapshot_date DESC
            LIMIT 50
        """, (dataset_id,))
        
        history = cursor.fetchall()
        
        # Get change events
        cursor.execute("""
            SELECT change_type, change_description, change_date, 
                   old_value, new_value, change_impact
            FROM dataset_changes
            WHERE dataset_id = ?
            ORDER BY change_date DESC
            LIMIT 20
        """, (dataset_id,))
        
        changes = cursor.fetchall()
        
        # Calculate analytics
        analytics = {
            'trends': calculate_trends(history),
            'quality_evolution': calculate_quality_evolution(history),
            'performance_metrics': calculate_performance_metrics(history),
            'change_patterns': analyze_change_patterns(changes),
            'reliability_indicators': calculate_reliability_indicators(history),
            'data_characteristics': analyze_data_characteristics(history)
        }
        
        conn.close()
        return jsonify(analytics)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@enhanced_dataset_bp.route('/api/dataset/<dataset_id>/comparison')
def get_dataset_comparison(dataset_id: str):
    """Get dataset comparison with similar datasets"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get current dataset info
        cursor.execute("""
            SELECT agency, resource_format, file_size, row_count, column_count
            FROM dataset_states
            WHERE dataset_id = ? AND snapshot_date = (
                SELECT MAX(snapshot_date) FROM dataset_states ds2 
                WHERE ds2.dataset_id = dataset_states.dataset_id
            )
        """, (dataset_id,))
        
        current = cursor.fetchone()
        if not current:
            return jsonify({'error': 'Dataset not found'}), 404
        
        # Find similar datasets
        cursor.execute("""
            SELECT dataset_id, title, agency, resource_format, file_size, 
                   row_count, column_count, analysis_quality_score
            FROM dataset_states ds
            WHERE ds.dataset_id != ? 
            AND ds.snapshot_date = (
                SELECT MAX(snapshot_date) FROM dataset_states ds2 
                WHERE ds2.dataset_id = ds.dataset_id
            )
            AND (
                ds.agency = ? OR 
                ds.resource_format = ? OR
                ABS(ds.file_size - ?) < ? * 0.5
            )
            ORDER BY 
                CASE WHEN ds.agency = ? THEN 1 ELSE 0 END +
                CASE WHEN ds.resource_format = ? THEN 1 ELSE 0 END +
                CASE WHEN ABS(ds.file_size - ?) < ? * 0.2 THEN 1 ELSE 0 END DESC
            LIMIT 10
        """, (dataset_id, current['agency'], current['resource_format'], 
              current['file_size'], current['file_size'], current['agency'],
              current['resource_format'], current['file_size'], current['file_size']))
        
        similar = cursor.fetchall()
        
        # Calculate comparison metrics
        comparison = {
            'current_dataset': {
                'dataset_id': dataset_id,
                'agency': current['agency'],
                'format': current['resource_format'],
                'file_size': current['file_size'],
                'rows': current['row_count'],
                'columns': current['column_count'],
                'quality_score': current['analysis_quality_score']
            },
            'similar_datasets': [
                {
                    'dataset_id': row['dataset_id'],
                    'title': row['title'],
                    'agency': row['agency'],
                    'format': row['resource_format'],
                    'file_size': row['file_size'],
                    'rows': row['row_count'],
                    'columns': row['column_count'],
                    'quality_score': row['analysis_quality_score'],
                    'similarity_score': calculate_similarity_score(current, row)
                } for row in similar
            ],
            'comparison_metrics': calculate_comparison_metrics(current, similar)
        }
        
        conn.close()
        return jsonify(comparison)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def calculate_trends(history: List[sqlite3.Row]) -> Dict[str, Any]:
    """Calculate trend analysis for dataset metrics"""
    if len(history) < 2:
        return {'trend': 'insufficient_data', 'direction': 'stable'}
    
    # Analyze row count trend
    row_counts = [h['row_count'] for h in history if h['row_count'] is not None]
    if len(row_counts) >= 2:
        row_trend = 'increasing' if row_counts[0] > row_counts[-1] else 'decreasing' if row_counts[0] < row_counts[-1] else 'stable'
    else:
        row_trend = 'unknown'
    
    # Analyze file size trend
    file_sizes = [h['file_size'] for h in history if h['file_size'] is not None]
    if len(file_sizes) >= 2:
        size_trend = 'increasing' if file_sizes[0] > file_sizes[-1] else 'decreasing' if file_sizes[0] < file_sizes[-1] else 'stable'
    else:
        size_trend = 'unknown'
    
    # Analyze quality trend
    quality_scores = [h['analysis_quality_score'] for h in history if h['analysis_quality_score'] is not None]
    if len(quality_scores) >= 2:
        quality_trend = 'improving' if quality_scores[0] > quality_scores[-1] else 'declining' if quality_scores[0] < quality_scores[-1] else 'stable'
    else:
        quality_trend = 'unknown'
    
    return {
        'row_count_trend': row_trend,
        'file_size_trend': size_trend,
        'quality_trend': quality_trend,
        'data_points': len(history),
        'time_span_days': (datetime.strptime(history[0]['snapshot_date'], '%Y-%m-%d') - 
                          datetime.strptime(history[-1]['snapshot_date'], '%Y-%m-%d')).days
    }

def calculate_quality_evolution(history: List[sqlite3.Row]) -> Dict[str, Any]:
    """Calculate quality evolution over time"""
    quality_data = []
    for h in history:
        if h['analysis_quality_score'] is not None:
            quality_data.append({
                'date': h['snapshot_date'],
                'score': h['analysis_quality_score'],
                'analyzed': bool(h['content_analyzed'])
            })
    
    if not quality_data:
        return {'evolution': 'no_quality_data', 'average_score': 0}
    
    scores = [q['score'] for q in quality_data]
    return {
        'evolution': 'improving' if len(scores) > 1 and scores[0] > scores[-1] else 'declining' if len(scores) > 1 and scores[0] < scores[-1] else 'stable',
        'average_score': sum(scores) / len(scores),
        'highest_score': max(scores),
        'lowest_score': min(scores),
        'score_variance': calculate_variance(scores),
        'data_points': len(quality_data)
    }

def calculate_performance_metrics(history: List[sqlite3.Row]) -> Dict[str, Any]:
    """Calculate performance metrics for the dataset"""
    if not history:
        return {}
    
    current = history[0]
    
    # Calculate availability percentage
    available_count = sum(1 for h in history if h['availability'] == 'available')
    availability_percentage = (available_count / len(history)) * 100
    
    # Calculate average response time (if available)
    response_times = [h['dimension_computation_time_ms'] for h in history if h['dimension_computation_time_ms'] is not None]
    avg_response_time = sum(response_times) / len(response_times) if response_times else None
    
    # Calculate data consistency
    row_counts = [h['row_count'] for h in history if h['row_count'] is not None]
    consistency_score = 100 - calculate_variance(row_counts) if row_counts else 0
    
    return {
        'availability_percentage': round(availability_percentage, 2),
        'average_response_time_ms': round(avg_response_time, 2) if avg_response_time else None,
        'data_consistency_score': round(max(0, min(100, consistency_score)), 2),
        'total_snapshots': len(history),
        'current_status': current['availability'],
        'current_quality_score': current['analysis_quality_score']
    }

def analyze_change_patterns(changes: List[sqlite3.Row]) -> Dict[str, Any]:
    """Analyze patterns in dataset changes"""
    if not changes:
        return {'pattern': 'no_changes', 'change_frequency': 0}
    
    change_types = {}
    for change in changes:
        change_type = change['change_type'] or 'unknown'
        change_types[change_type] = change_types.get(change_type, 0) + 1
    
    # Calculate change frequency (changes per day)
    if len(changes) > 1:
        first_change = datetime.strptime(changes[-1]['change_date'], '%Y-%m-%d')
        last_change = datetime.strptime(changes[0]['change_date'], '%Y-%m-%d')
        days = (last_change - first_change).days
        change_frequency = len(changes) / max(1, days) if days > 0 else 0
    else:
        change_frequency = 0
    
    return {
        'change_frequency_per_day': round(change_frequency, 3),
        'total_changes': len(changes),
        'change_type_distribution': change_types,
        'most_common_change': max(change_types.items(), key=lambda x: x[1])[0] if change_types else 'none',
        'change_pattern': 'frequent' if change_frequency > 1 else 'occasional' if change_frequency > 0.1 else 'rare'
    }

def calculate_reliability_indicators(history: List[sqlite3.Row]) -> Dict[str, Any]:
    """Calculate reliability indicators for the dataset"""
    if not history:
        return {'reliability_score': 0, 'indicators': []}
    
    indicators = []
    score = 0
    
    # Availability indicator
    available_count = sum(1 for h in history if h['availability'] == 'available')
    availability_score = (available_count / len(history)) * 100
    indicators.append({
        'name': 'Availability',
        'score': round(availability_score, 2),
        'description': f'Available {available_count}/{len(history)} snapshots'
    })
    score += availability_score * 0.4
    
    # Consistency indicator
    row_counts = [h['row_count'] for h in history if h['row_count'] is not None]
    if row_counts:
        consistency_score = 100 - calculate_variance(row_counts)
        indicators.append({
            'name': 'Data Consistency',
            'score': round(max(0, min(100, consistency_score)), 2),
            'description': f'Row count variance: {calculate_variance(row_counts):.2f}'
        })
        score += consistency_score * 0.3
    
    # Quality indicator
    quality_scores = [h['analysis_quality_score'] for h in history if h['analysis_quality_score'] is not None]
    if quality_scores:
        avg_quality = sum(quality_scores) / len(quality_scores)
        indicators.append({
            'name': 'Data Quality',
            'score': round(avg_quality, 2),
            'description': f'Average quality score: {avg_quality:.2f}'
        })
        score += avg_quality * 0.3
    
    return {
        'reliability_score': round(min(100, score), 2),
        'indicators': indicators,
        'overall_assessment': 'high' if score >= 80 else 'medium' if score >= 60 else 'low'
    }

def analyze_data_characteristics(history: List[sqlite3.Row]) -> Dict[str, Any]:
    """Analyze data characteristics and patterns"""
    if not history:
        return {}
    
    current = history[0]
    
    # File size analysis
    file_sizes = [h['file_size'] for h in history if h['file_size'] is not None]
    size_analysis = {
        'current_size_mb': round((current['file_size'] or 0) / (1024 * 1024), 2),
        'average_size_mb': round(sum(file_sizes) / len(file_sizes) / (1024 * 1024), 2) if file_sizes else 0,
        'size_category': categorize_file_size(current['file_size'] or 0)
    }
    
    # Data volume analysis
    row_counts = [h['row_count'] for h in history if h['row_count'] is not None and h['row_count'] > 0]
    volume_analysis = {
        'current_rows': current['row_count'] or 0,
        'current_columns': current['column_count'] or 0,
        'average_rows': round(sum(row_counts) / len(row_counts), 0) if row_counts else 0,
        'data_volume_category': categorize_data_volume(current['row_count'] or 0)
    }
    
    # Format analysis
    formats = [h['resource_format'] for h in history if h['resource_format']]
    format_analysis = {
        'current_format': current['resource_format'],
        'format_stability': len(set(formats)) == 1 if formats else True,
        'format_changes': len(set(formats)) - 1 if formats else 0
    }
    
    return {
        'file_size': size_analysis,
        'data_volume': volume_analysis,
        'format': format_analysis,
        'update_frequency': len(history),
        'data_freshness': calculate_data_freshness(current['snapshot_date'])
    }

def calculate_similarity_score(current: sqlite3.Row, other: sqlite3.Row) -> float:
    """Calculate similarity score between two datasets"""
    score = 0
    
    # Agency match
    if current['agency'] == other['agency']:
        score += 30
    
    # Format match
    if current['resource_format'] == other['resource_format']:
        score += 25
    
    # Size similarity
    if current['file_size'] and other['file_size']:
        size_diff = abs(current['file_size'] - other['file_size']) / max(current['file_size'], other['file_size'])
        score += max(0, 25 * (1 - size_diff))
    
    # Row count similarity
    if current['row_count'] and other['row_count'] and current['row_count'] > 0 and other['row_count'] > 0:
        row_diff = abs(current['row_count'] - other['row_count']) / max(current['row_count'], other['row_count'])
        score += max(0, 20 * (1 - row_diff))
    
    return min(100, score)

def calculate_comparison_metrics(current: sqlite3.Row, similar: List[sqlite3.Row]) -> Dict[str, Any]:
    """Calculate comparison metrics with similar datasets"""
    if not similar:
        return {}
    
    # Calculate averages for similar datasets
    similar_sizes = [s['file_size'] for s in similar if s['file_size']]
    similar_rows = [s['row_count'] for s in similar if s['row_count']]
    similar_quality = [s['analysis_quality_score'] for s in similar if s['analysis_quality_score']]
    
    return {
        'size_comparison': {
            'current_size_mb': round((current['file_size'] or 0) / (1024 * 1024), 2),
            'average_similar_size_mb': round(sum(similar_sizes) / len(similar_sizes) / (1024 * 1024), 2) if similar_sizes else 0,
            'size_percentile': calculate_percentile(current['file_size'] or 0, similar_sizes)
        },
        'quality_comparison': {
            'current_quality': current['analysis_quality_score'],
            'average_similar_quality': round(sum(similar_quality) / len(similar_quality), 2) if similar_quality else 0,
            'quality_percentile': calculate_percentile(current['analysis_quality_score'] or 0, similar_quality)
        },
        'volume_comparison': {
            'current_rows': current['row_count'] or 0,
            'average_similar_rows': round(sum(similar_rows) / len(similar_rows), 0) if similar_rows else 0,
            'row_percentile': calculate_percentile(current['row_count'] or 0, similar_rows)
        }
    }

def calculate_variance(values: List[float]) -> float:
    """Calculate variance of a list of values"""
    if len(values) < 2:
        return 0
    
    mean = sum(values) / len(values)
    variance = sum((x - mean) ** 2 for x in values) / len(values)
    return variance

def categorize_file_size(file_size: int) -> str:
    """Categorize file size"""
    if file_size < 1024:
        return 'Tiny (< 1KB)'
    elif file_size < 1024 * 1024:
        return 'Small (< 1MB)'
    elif file_size < 10 * 1024 * 1024:
        return 'Medium (< 10MB)'
    elif file_size < 100 * 1024 * 1024:
        return 'Large (< 100MB)'
    else:
        return 'Very Large (> 100MB)'

def categorize_data_volume(row_count: int) -> str:
    """Categorize data volume by row count"""
    if row_count == 0:
        return 'Empty'
    elif row_count < 100:
        return 'Small (< 100 rows)'
    elif row_count < 1000:
        return 'Medium (< 1K rows)'
    elif row_count < 10000:
        return 'Large (< 10K rows)'
    elif row_count < 100000:
        return 'Very Large (< 100K rows)'
    else:
        return 'Massive (> 100K rows)'

def calculate_data_freshness(snapshot_date: str) -> str:
    """Calculate data freshness"""
    try:
        snapshot = datetime.strptime(snapshot_date, '%Y-%m-%d')
        days_old = (datetime.now() - snapshot).days
        
        if days_old == 0:
            return 'Today'
        elif days_old == 1:
            return 'Yesterday'
        elif days_old < 7:
            return f'{days_old} days ago'
        elif days_old < 30:
            return f'{days_old // 7} weeks ago'
        elif days_old < 365:
            return f'{days_old // 30} months ago'
        else:
            return f'{days_old // 365} years ago'
    except:
        return 'Unknown'

def calculate_percentile(value: float, values: List[float]) -> float:
    """Calculate percentile of value in list of values"""
    if not values:
        return 0
    
    sorted_values = sorted(values)
    count = sum(1 for v in sorted_values if v <= value)
    return (count / len(sorted_values)) * 100
