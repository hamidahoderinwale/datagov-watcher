"""
Dataset Enhancer
Gathers additional information about datasets from various sources
"""

import requests
import json
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional, Any
import re
from urllib.parse import urlparse

class DatasetEnhancer:
    """Enhances dataset information with additional metadata"""
    
    def __init__(self, db_path: str = "datasets.db"):
        self.db_path = db_path
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Dataset State Historian/1.0'
        })
    
    def enhance_dataset_info(self, dataset_id: str) -> Dict[str, Any]:
        """Enhance dataset information with additional metadata"""
        conn = sqlite3.connect(self.db_path)
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
            conn.close()
            return {}
        
        enhanced_info = {
            'dataset_id': dataset_id,
            'basic_info': {
                'title': dataset[12],  # title column
                'agency': dataset[13],  # agency column
                'url': dataset[14],  # url column
                'description': dataset[15],  # description column
                'last_modified': dataset[16],  # last_modified column
                'source': dataset[17]  # source column
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
            'schema_info': {
                'schema_hash': dataset[6],  # schema_hash column
                'content_hash': dataset[7],  # content_hash column
                'schema_columns': json.loads(dataset[25]) if dataset[25] else [],  # schema_columns column
                'schema_dtypes': json.loads(dataset[26]) if dataset[26] else {}  # schema_dtypes column
            },
            'enhanced_metadata': {}
        }
        
        # Try to gather additional information
        if dataset[14]:  # url column
            enhanced_info['enhanced_metadata'] = self._gather_url_metadata(dataset[14])
        
        # Analyze data patterns
        enhanced_info['data_patterns'] = self._analyze_data_patterns(dataset)
        
        # Calculate additional metrics
        enhanced_info['metrics'] = self._calculate_metrics(dataset)
        
        conn.close()
        return enhanced_info
    
    def _gather_url_metadata(self, url: str) -> Dict[str, Any]:
        """Gather metadata from the dataset URL"""
        try:
            response = self.session.head(url, timeout=10)
            metadata = {
                'url_accessible': response.status_code == 200,
                'status_code': response.status_code,
                'content_type': response.headers.get('content-type', ''),
                'content_length': response.headers.get('content-length', ''),
                'last_modified': response.headers.get('last-modified', ''),
                'etag': response.headers.get('etag', ''),
                'server': response.headers.get('server', ''),
                'cache_control': response.headers.get('cache-control', ''),
                'expires': response.headers.get('expires', ''),
                'content_encoding': response.headers.get('content-encoding', ''),
                'content_language': response.headers.get('content-language', ''),
                'response_time_ms': response.elapsed.total_seconds() * 1000
            }
            
            # Analyze URL structure
            parsed_url = urlparse(url)
            metadata['url_analysis'] = {
                'domain': parsed_url.netloc,
                'path_depth': len([p for p in parsed_url.path.split('/') if p]),
                'has_query': bool(parsed_url.query),
                'has_fragment': bool(parsed_url.fragment),
                'is_https': parsed_url.scheme == 'https',
                'file_extension': self._get_file_extension(parsed_url.path)
            }
            
            return metadata
            
        except Exception as e:
            return {
                'url_accessible': False,
                'error': str(e),
                'url_analysis': self._analyze_url_structure(url)
            }
    
    def _analyze_url_structure(self, url: str) -> Dict[str, Any]:
        """Analyze URL structure without making requests"""
        try:
            parsed_url = urlparse(url)
            return {
                'domain': parsed_url.netloc,
                'path_depth': len([p for p in parsed_url.path.split('/') if p]),
                'has_query': bool(parsed_url.query),
                'has_fragment': bool(parsed_url.fragment),
                'is_https': parsed_url.scheme == 'https',
                'file_extension': self._get_file_extension(parsed_url.path)
            }
        except:
            return {}
    
    def _get_file_extension(self, path: str) -> str:
        """Extract file extension from path"""
        if '.' in path:
            return path.split('.')[-1].lower()
        return ''
    
    def _analyze_data_patterns(self, dataset) -> Dict[str, Any]:
        """Analyze data patterns and characteristics"""
        patterns = {
            'data_volume': self._categorize_data_volume(dataset[10]),  # file_size column
            'format_category': self._categorize_format(dataset[17]),  # resource_format column
            'update_frequency': self._analyze_update_frequency(dataset[1]),  # dataset_id column
            'stability_score': self._calculate_stability_score(dataset[1]),  # dataset_id column
            'complexity_score': self._calculate_complexity_score(dataset)
        }
        return patterns
    
    def _categorize_data_volume(self, file_size: int) -> str:
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
    
    def _categorize_format(self, format_str: str) -> str:
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
    
    def _analyze_update_frequency(self, dataset_id: str) -> str:
        """Analyze how frequently the dataset is updated"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT COUNT(*) as snapshot_count,
                   MIN(snapshot_date) as first_seen,
                   MAX(snapshot_date) as last_seen
            FROM dataset_states
            WHERE dataset_id = ?
        """, (dataset_id,))
        
        result = cursor.fetchone()
        conn.close()
        
        if not result or not result['snapshot_count']:
            return 'Unknown'
        
        snapshot_count = result['snapshot_count']
        if snapshot_count == 1:
            return 'Static'
        elif snapshot_count <= 5:
            return 'Rarely Updated'
        elif snapshot_count <= 20:
            return 'Occasionally Updated'
        elif snapshot_count <= 100:
            return 'Frequently Updated'
        else:
            return 'Very Active'
    
    def _calculate_stability_score(self, dataset_id: str) -> float:
        """Calculate dataset stability score (0-100)"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT content_hash, schema_hash, row_count, column_count
            FROM dataset_states
            WHERE dataset_id = ?
            ORDER BY snapshot_date DESC
            LIMIT 10
        """, (dataset_id,))
        
        snapshots = cursor.fetchall()
        conn.close()
        
        if len(snapshots) < 2:
            return 100.0  # Single snapshot is considered stable
        
        # Calculate stability based on content and schema changes
        content_changes = 0
        schema_changes = 0
        dimension_changes = 0
        
        for i in range(1, len(snapshots)):
            prev = snapshots[i-1]
            curr = snapshots[i]
            
            if prev['content_hash'] != curr['content_hash']:
                content_changes += 1
            if prev['schema_hash'] != curr['schema_hash']:
                schema_changes += 1
            if (prev['row_count'] != curr['row_count'] or 
                prev['column_count'] != curr['column_count']):
                dimension_changes += 1
        
        total_snapshots = len(snapshots) - 1
        stability = 100 - (
            (content_changes / total_snapshots) * 50 +
            (schema_changes / total_snapshots) * 30 +
            (dimension_changes / total_snapshots) * 20
        )
        
        return max(0, min(100, stability))
    
    def _calculate_complexity_score(self, dataset) -> float:
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
        
        # Schema complexity
        if dataset[25]:  # schema_columns column
            try:
                columns = json.loads(dataset[25])
                score += min(30, len(columns) * 2)
            except:
                pass
        
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
    
    def _calculate_metrics(self, dataset) -> Dict[str, Any]:
        """Calculate additional metrics for the dataset"""
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
