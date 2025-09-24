"""
Search API - Comprehensive search across datasets, agencies, and tags
"""

from flask import Blueprint, jsonify, request
import sqlite3
import json
from datetime import datetime
import re

search_bp = Blueprint('search', __name__, url_prefix='/api/search')

def get_db_connection():
    """Get database connection"""
    return sqlite3.connect('datasets.db')

@search_bp.route('/')
def search_all():
    """Search across all content types"""
    query = request.args.get('q', '').strip()
    content_type = request.args.get('type', 'all')
    limit = int(request.args.get('limit', 50))
    offset = int(request.args.get('offset', 0))
    
    if not query:
        return jsonify({'error': 'Query parameter is required'}), 400
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        results = {
            'query': query,
            'total_results': 0,
            'results': [],
            'facets': {
                'datasets': 0,
                'agencies': 0
            }
        }
        
        # Search datasets
        if content_type in ['all', 'datasets']:
            dataset_results = search_datasets(cursor, query, limit, offset)
            results['results'].extend(dataset_results)
            results['facets']['datasets'] = len(dataset_results)
        
        # Search agencies
        if content_type in ['all', 'agencies']:
            agency_results = search_agencies(cursor, query, limit, offset)
            results['results'].extend(agency_results)
            results['facets']['agencies'] = len(agency_results)
        
        results['total_results'] = len(results['results'])
        
        conn.close()
        return jsonify(results)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def search_datasets(cursor, query, limit, offset):
    """Search datasets by title, description, and agency"""
    cursor.execute("""
        SELECT 
            d.id,
            d.title,
            d.description,
            d.agency,
            d.url,
            ds.availability,
            ds.snapshot_date,
            ds.row_count,
            ds.column_count
        FROM datasets d
        LEFT JOIN dataset_states ds ON d.id = ds.dataset_id
        WHERE (
            d.title LIKE ? OR 
            d.description LIKE ? OR 
            d.agency LIKE ?
        )
        AND ds.snapshot_date = (
            SELECT MAX(snapshot_date) 
            FROM dataset_states ds2 
            WHERE ds2.dataset_id = d.id
        )
        ORDER BY d.title
        LIMIT ? OFFSET ?
    """, (f'%{query}%', f'%{query}%', f'%{query}%', limit, offset))
    
    results = []
    for row in cursor.fetchall():
        results.append({
            'type': 'dataset',
            'id': row[0],
            'title': row[1],
            'description': row[2],
            'agency': row[3],
            'url': row[4],
            'status': row[5] or 'unknown',
            'last_updated': row[6],
            'row_count': row[7],
            'column_count': row[8],
            'relevance_score': calculate_relevance_score(query, row[1], row[2], row[3])
        })
    
    return sorted(results, key=lambda x: x['relevance_score'], reverse=True)

def search_agencies(cursor, query, limit, offset):
    """Search agencies by name"""
    cursor.execute("""
        SELECT 
            d.agency,
            COUNT(*) as dataset_count,
            COUNT(CASE WHEN ds.availability = 'available' THEN 1 END) as available_count,
            COUNT(CASE WHEN ds.availability = 'unavailable' THEN 1 END) as unavailable_count
        FROM datasets d
        LEFT JOIN dataset_states ds ON d.id = ds.dataset_id
        WHERE d.agency LIKE ?
        AND ds.snapshot_date = (
            SELECT MAX(snapshot_date) 
            FROM dataset_states ds2 
            WHERE ds2.dataset_id = d.id
        )
        GROUP BY d.agency
        ORDER BY dataset_count DESC
        LIMIT ? OFFSET ?
    """, (f'%{query}%', limit, offset))
    
    results = []
    for row in cursor.fetchall():
        results.append({
            'type': 'agency',
            'name': row[0],
            'dataset_count': row[1],
            'available_count': row[2],
            'unavailable_count': row[3],
            'relevance_score': calculate_relevance_score(query, row[0])
        })
    
    return sorted(results, key=lambda x: x['relevance_score'], reverse=True)

def calculate_relevance_score(query, *text_fields):
    """Calculate relevance score for search results"""
    query_lower = query.lower()
    score = 0
    
    for field in text_fields:
        if field:
            field_lower = field.lower()
            # Exact match gets highest score
            if query_lower == field_lower:
                score += 100
            # Starts with query gets high score
            elif field_lower.startswith(query_lower):
                score += 50
            # Contains query gets medium score
            elif query_lower in field_lower:
                score += 25
            # Word boundary match gets lower score
            elif re.search(r'\b' + re.escape(query_lower) + r'\b', field_lower):
                score += 10
    
    return score

@search_bp.route('/suggestions')
def get_suggestions():
    """Get search suggestions as user types"""
    query = request.args.get('q', '').strip()
    limit = int(request.args.get('limit', 10))
    
    if len(query) < 2:
        return jsonify({'suggestions': []})
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        suggestions = []
        
        # Get dataset title suggestions
        cursor.execute("""
            SELECT DISTINCT title
            FROM datasets
            WHERE title LIKE ?
            ORDER BY title
            LIMIT ?
        """, (f'%{query}%', limit))
        
        for row in cursor.fetchall():
            suggestions.append({
                'type': 'dataset',
                'text': row[0],
                'category': 'Datasets'
            })
        
        # Get agency suggestions
        cursor.execute("""
            SELECT DISTINCT agency
            FROM datasets
            WHERE agency LIKE ?
            ORDER BY agency
            LIMIT ?
        """, (f'%{query}%', limit))
        
        for row in cursor.fetchall():
            suggestions.append({
                'type': 'agency',
                'text': row[0],
                'category': 'Agencies'
            })
        
        conn.close()
        
        # Remove duplicates and limit results
        seen = set()
        unique_suggestions = []
        for suggestion in suggestions:
            key = (suggestion['type'], suggestion['text'])
            if key not in seen:
                seen.add(key)
                unique_suggestions.append(suggestion)
                if len(unique_suggestions) >= limit:
                    break
        
        return jsonify({'suggestions': unique_suggestions})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@search_bp.route('/filters')
def get_search_filters():
    """Get available search filters"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get status distribution
        cursor.execute("""
            SELECT COALESCE(availability, 'unknown') as status, COUNT(*) 
            FROM dataset_states 
            WHERE snapshot_date = (
                SELECT MAX(snapshot_date) 
                FROM dataset_states ds2 
                WHERE ds2.dataset_id = dataset_states.dataset_id
            )
            GROUP BY COALESCE(availability, 'unknown')
        """)
        status_filters = dict(cursor.fetchall())
        
        # Get agency list
        cursor.execute("""
            SELECT agency, COUNT(*) as count
            FROM datasets d
            LEFT JOIN dataset_states ds ON d.id = ds.dataset_id
            WHERE ds.snapshot_date = (
                SELECT MAX(snapshot_date) 
                FROM dataset_states ds2 
                WHERE ds2.dataset_id = d.id
            )
            GROUP BY agency
            ORDER BY count DESC
            LIMIT 20
        """)
        agency_filters = [{'name': row[0], 'count': row[1]} for row in cursor.fetchall()]
        
        conn.close()
        
        return jsonify({
            'status': status_filters,
            'agencies': agency_filters
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500