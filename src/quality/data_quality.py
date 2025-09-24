"""
Data Quality Assessment and Validation System
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import re
from typing import Dict, List, Tuple, Any

class DataQualityAssessor:
    def __init__(self, db_path='datasets.db'):
        self.db_path = db_path
    
    def assess_dataset_quality(self, dataset_id: str) -> Dict[str, Any]:
        """Assess data quality for a specific dataset"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get dataset information
            cursor.execute('''
                SELECT d.title, d.agency, d.url, d.description,
                       ds.availability, ds.row_count, ds.column_count,
                       ds.file_size, ds.resource_format, ds.schema_columns,
                       ds.schema_dtypes, ds.snapshot_date
                FROM datasets d
                LEFT JOIN dataset_states ds ON d.id = ds.dataset_id
                WHERE d.id = ? AND ds.snapshot_date = (
                    SELECT MAX(snapshot_date) 
                    FROM dataset_states ds2 
                    WHERE ds2.dataset_id = d.id
                )
            ''', (dataset_id,))
            
            dataset_info = cursor.fetchone()
            if not dataset_info:
                return {'error': 'Dataset not found'}
            
            title, agency, url, description, availability, row_count, column_count, file_size, resource_format, schema_columns, schema_dtypes, snapshot_date = dataset_info
            
            # Parse schema information
            columns = json.loads(schema_columns) if schema_columns else []
            dtypes = json.loads(schema_dtypes) if schema_dtypes else []
            
            # Calculate quality metrics
            quality_metrics = {
                'completeness': self._assess_completeness(dataset_info),
                'accuracy': self._assess_accuracy(dataset_info),
                'consistency': self._assess_consistency(dataset_info),
                'timeliness': self._assess_timeliness(snapshot_date),
                'validity': self._assess_validity(dataset_info),
                'uniqueness': self._assess_uniqueness(dataset_info),
                'reliability': self._assess_reliability(dataset_info)
            }
            
            # Calculate overall quality score
            overall_score = self._calculate_overall_score(quality_metrics)
            
            # Generate quality report
            quality_report = {
                'dataset_id': dataset_id,
                'title': title,
                'agency': agency,
                'overall_score': overall_score,
                'quality_metrics': quality_metrics,
                'recommendations': self._generate_recommendations(quality_metrics),
                'assessment_date': datetime.now().isoformat(),
                'status': self._get_quality_status(overall_score)
            }
            
            conn.close()
            return quality_report
            
        except Exception as e:
            return {'error': str(e)}
    
    def _assess_completeness(self, dataset_info) -> Dict[str, Any]:
        """Assess data completeness"""
        title, agency, url, description, availability, row_count, column_count, file_size, resource_format, schema_columns, schema_dtypes, snapshot_date = dataset_info
        
        completeness_score = 0
        details = []
        
        # Check required fields
        required_fields = {
            'title': title,
            'agency': agency,
            'url': url,
            'description': description
        }
        
        present_fields = sum(1 for field in required_fields.values() if field and field.strip())
        completeness_score += (present_fields / len(required_fields)) * 40
        
        if present_fields == len(required_fields):
            details.append("All required fields are present")
        else:
            missing_fields = [field for field, value in required_fields.items() if not value or not value.strip()]
            details.append(f"Missing required fields: {', '.join(missing_fields)}")
        
        # Check data dimensions
        if row_count is not None and row_count > 0:
            completeness_score += 30
            details.append(f"Dataset has {row_count:,} rows")
        else:
            details.append("Row count not available or zero")
        
        if column_count is not None and column_count > 0:
            completeness_score += 20
            details.append(f"Dataset has {column_count} columns")
        else:
            details.append("Column count not available or zero")
        
        # Check schema information
        if schema_columns and len(json.loads(schema_columns)) > 0:
            completeness_score += 10
            details.append("Schema information available")
        else:
            details.append("Schema information missing")
        
        return {
            'score': min(completeness_score, 100),
            'details': details,
            'status': 'excellent' if completeness_score >= 90 else 'good' if completeness_score >= 70 else 'poor'
        }
    
    def _assess_accuracy(self, dataset_info) -> Dict[str, Any]:
        """Assess data accuracy"""
        title, agency, url, description, availability, row_count, column_count, file_size, resource_format, schema_columns, schema_dtypes, snapshot_date = dataset_info
        
        accuracy_score = 0
        details = []
        
        # Check URL validity
        if url and self._is_valid_url(url):
            accuracy_score += 30
            details.append("Valid URL format")
        else:
            details.append("Invalid or missing URL")
        
        # Check agency name format
        if agency and self._is_valid_agency_name(agency):
            accuracy_score += 25
            details.append("Valid agency name format")
        else:
            details.append("Agency name format could be improved")
        
        # Check title quality
        if title and len(title.strip()) > 10:
            accuracy_score += 25
            details.append("Title is descriptive")
        else:
            details.append("Title could be more descriptive")
        
        # Check description quality
        if description and len(description.strip()) > 50:
            accuracy_score += 20
            details.append("Description is detailed")
        else:
            details.append("Description could be more detailed")
        
        return {
            'score': min(accuracy_score, 100),
            'details': details,
            'status': 'excellent' if accuracy_score >= 90 else 'good' if accuracy_score >= 70 else 'poor'
        }
    
    def _assess_consistency(self, dataset_info) -> Dict[str, Any]:
        """Assess data consistency"""
        title, agency, url, description, availability, row_count, column_count, file_size, resource_format, schema_columns, schema_dtypes, snapshot_date = dataset_info
        
        consistency_score = 0
        details = []
        
        # Check format consistency
        if resource_format and resource_format.upper() in ['CSV', 'JSON', 'XML', 'RDF', 'EXCEL']:
            consistency_score += 30
            details.append(f"Standard format: {resource_format.upper()}")
        else:
            details.append("Non-standard or missing format")
        
        # Check data type consistency
        if schema_dtypes:
            dtypes = json.loads(schema_dtypes)
            if len(set(dtypes)) > 1:  # Multiple data types
                consistency_score += 25
                details.append("Mixed data types detected")
            else:
                consistency_score += 15
                details.append("Consistent data types")
        
        # Check naming consistency
        if title and agency:
            if self._is_consistent_naming(title, agency):
                consistency_score += 25
                details.append("Consistent naming conventions")
            else:
                details.append("Naming conventions could be improved")
        
        # Check size consistency
        if file_size and file_size > 0:
            consistency_score += 20
            details.append(f"File size: {file_size:,} bytes")
        else:
            details.append("File size not available")
        
        return {
            'score': min(consistency_score, 100),
            'details': details,
            'status': 'excellent' if consistency_score >= 90 else 'good' if consistency_score >= 70 else 'poor'
        }
    
    def _assess_timeliness(self, snapshot_date) -> Dict[str, Any]:
        """Assess data timeliness"""
        if not snapshot_date:
            return {
                'score': 0,
                'details': ['No snapshot date available'],
                'status': 'poor'
            }
        
        try:
            snapshot_dt = datetime.fromisoformat(snapshot_date.replace('Z', '+00:00'))
            days_old = (datetime.now() - snapshot_dt).days
            
            if days_old <= 1:
                timeliness_score = 100
                status = 'excellent'
                details = ['Data is very recent (within 1 day)']
            elif days_old <= 7:
                timeliness_score = 90
                status = 'excellent'
                details = ['Data is recent (within 1 week)']
            elif days_old <= 30:
                timeliness_score = 70
                status = 'good'
                details = ['Data is moderately recent (within 1 month)']
            elif days_old <= 90:
                timeliness_score = 50
                status = 'fair'
                details = ['Data is somewhat outdated (within 3 months)']
            else:
                timeliness_score = 20
                status = 'poor'
                details = ['Data is outdated (over 3 months old)']
            
            details.append(f'Last updated: {days_old} days ago')
            
            return {
                'score': timeliness_score,
                'details': details,
                'status': status
            }
            
        except Exception as e:
            return {
                'score': 0,
                'details': [f'Error parsing date: {str(e)}'],
                'status': 'poor'
            }
    
    def _assess_validity(self, dataset_info) -> Dict[str, Any]:
        """Assess data validity"""
        title, agency, url, description, availability, row_count, column_count, file_size, resource_format, schema_columns, schema_dtypes, snapshot_date = dataset_info
        
        validity_score = 0
        details = []
        
        # Check availability
        if availability == 'available':
            validity_score += 40
            details.append("Dataset is currently available")
        else:
            details.append("Dataset is not available")
        
        # Check data dimensions validity
        if row_count is not None and row_count > 0:
            validity_score += 30
            details.append(f"Valid row count: {row_count:,}")
        else:
            details.append("Invalid or missing row count")
        
        if column_count is not None and column_count > 0:
            validity_score += 20
            details.append(f"Valid column count: {column_count}")
        else:
            details.append("Invalid or missing column count")
        
        # Check file size validity
        if file_size is not None and file_size > 0:
            validity_score += 10
            details.append(f"Valid file size: {file_size:,} bytes")
        else:
            details.append("Invalid or missing file size")
        
        return {
            'score': min(validity_score, 100),
            'details': details,
            'status': 'excellent' if validity_score >= 90 else 'good' if validity_score >= 70 else 'poor'
        }
    
    def _assess_uniqueness(self, dataset_info) -> Dict[str, Any]:
        """Assess data uniqueness"""
        title, agency, url, description, availability, row_count, column_count, file_size, resource_format, schema_columns, schema_dtypes, snapshot_date = dataset_info
        
        uniqueness_score = 0
        details = []
        
        # Check for unique identifier
        if url and len(url) > 10:
            uniqueness_score += 50
            details.append("Unique URL identifier present")
        else:
            details.append("Missing or invalid URL identifier")
        
        # Check title uniqueness
        if title and len(title.strip()) > 5:
            uniqueness_score += 30
            details.append("Descriptive title present")
        else:
            details.append("Title could be more unique")
        
        # Check agency uniqueness
        if agency and len(agency.strip()) > 3:
            uniqueness_score += 20
            details.append("Agency identifier present")
        else:
            details.append("Agency identifier missing")
        
        return {
            'score': min(uniqueness_score, 100),
            'details': details,
            'status': 'excellent' if uniqueness_score >= 90 else 'good' if uniqueness_score >= 70 else 'poor'
        }
    
    def _assess_reliability(self, dataset_info) -> Dict[str, Any]:
        """Assess data reliability"""
        title, agency, url, description, availability, row_count, column_count, file_size, resource_format, schema_columns, schema_dtypes, snapshot_date = dataset_info
        
        reliability_score = 0
        details = []
        
        # Check data source reliability
        if agency and self._is_reliable_agency(agency):
            reliability_score += 40
            details.append("Reliable government agency source")
        else:
            reliability_score += 20
            details.append("Agency source reliability unknown")
        
        # Check data format reliability
        if resource_format and resource_format.upper() in ['CSV', 'JSON', 'XML']:
            reliability_score += 30
            details.append("Reliable data format")
        else:
            details.append("Data format reliability unknown")
        
        # Check data completeness for reliability
        if row_count and column_count and row_count > 0 and column_count > 0:
            reliability_score += 30
            details.append("Data dimensions available for reliability assessment")
        else:
            details.append("Insufficient data for reliability assessment")
        
        return {
            'score': min(reliability_score, 100),
            'details': details,
            'status': 'excellent' if reliability_score >= 90 else 'good' if reliability_score >= 70 else 'poor'
        }
    
    def _calculate_overall_score(self, quality_metrics: Dict[str, Any]) -> float:
        """Calculate overall quality score"""
        weights = {
            'completeness': 0.20,
            'accuracy': 0.15,
            'consistency': 0.15,
            'timeliness': 0.20,
            'validity': 0.15,
            'uniqueness': 0.10,
            'reliability': 0.05
        }
        
        weighted_score = sum(
            quality_metrics[metric]['score'] * weight
            for metric, weight in weights.items()
        )
        
        return round(weighted_score, 2)
    
    def _generate_recommendations(self, quality_metrics: Dict[str, Any]) -> List[str]:
        """Generate quality improvement recommendations"""
        recommendations = []
        
        for metric, data in quality_metrics.items():
            if data['score'] < 70:
                if metric == 'completeness':
                    recommendations.append("Add missing metadata fields (description, schema information)")
                elif metric == 'accuracy':
                    recommendations.append("Improve URL format and agency name standardization")
                elif metric == 'consistency':
                    recommendations.append("Standardize data formats and naming conventions")
                elif metric == 'timeliness':
                    recommendations.append("Update dataset more frequently")
                elif metric == 'validity':
                    recommendations.append("Ensure dataset is accessible and has valid dimensions")
                elif metric == 'uniqueness':
                    recommendations.append("Add unique identifiers and improve titles")
                elif metric == 'reliability':
                    recommendations.append("Improve data source reliability and format standards")
        
        return recommendations
    
    def _get_quality_status(self, overall_score: float) -> str:
        """Get quality status based on overall score"""
        if overall_score >= 90:
            return 'excellent'
        elif overall_score >= 80:
            return 'good'
        elif overall_score >= 70:
            return 'fair'
        elif overall_score >= 60:
            return 'poor'
        else:
            return 'critical'
    
    def _is_valid_url(self, url: str) -> bool:
        """Check if URL is valid"""
        url_pattern = re.compile(
            r'^https?://'  # http:// or https://
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain...
            r'localhost|'  # localhost...
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
            r'(?::\d+)?'  # optional port
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)
        return url_pattern.match(url) is not None
    
    def _is_valid_agency_name(self, agency: str) -> bool:
        """Check if agency name is valid"""
        return len(agency.strip()) > 3 and any(char.isalpha() for char in agency)
    
    def _is_consistent_naming(self, title: str, agency: str) -> bool:
        """Check if naming is consistent"""
        return len(title.strip()) > 5 and len(agency.strip()) > 3
    
    def _is_reliable_agency(self, agency: str) -> bool:
        """Check if agency is reliable"""
        reliable_agencies = [
            'census bureau', 'nasa', 'epa', 'department of', 'usda', 'hhs',
            'department of commerce', 'department of energy', 'department of transportation'
        ]
        return any(reliable in agency.lower() for reliable in reliable_agencies)
    
    def get_quality_summary(self) -> Dict[str, Any]:
        """Get overall data quality summary"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get total datasets
            cursor.execute('SELECT COUNT(*) FROM datasets')
            total_datasets = cursor.fetchone()[0]
            
            # Get available datasets
            cursor.execute('''
                SELECT COUNT(*) FROM datasets d
                JOIN dataset_states ds ON d.id = ds.dataset_id
                WHERE ds.availability = 'available'
                AND ds.snapshot_date = (
                    SELECT MAX(snapshot_date) 
                    FROM dataset_states ds2 
                    WHERE ds2.dataset_id = d.id
                )
            ''')
            available_datasets = cursor.fetchone()[0]
            
            # Get datasets with complete metadata
            cursor.execute('''
                SELECT COUNT(*) FROM datasets d
                JOIN dataset_states ds ON d.id = ds.dataset_id
                WHERE d.title IS NOT NULL AND d.title != ''
                AND d.agency IS NOT NULL AND d.agency != ''
                AND d.url IS NOT NULL AND d.url != ''
                AND d.description IS NOT NULL AND d.description != ''
                AND ds.snapshot_date = (
                    SELECT MAX(snapshot_date) 
                    FROM dataset_states ds2 
                    WHERE ds2.dataset_id = d.id
                )
            ''')
            complete_datasets = cursor.fetchone()[0]
            
            conn.close()
            
            return {
                'total_datasets': total_datasets,
                'available_datasets': available_datasets,
                'complete_datasets': complete_datasets,
                'availability_rate': round((available_datasets / total_datasets) * 100, 2) if total_datasets > 0 else 0,
                'completeness_rate': round((complete_datasets / total_datasets) * 100, 2) if total_datasets > 0 else 0,
                'overall_quality_score': round(((available_datasets + complete_datasets) / (total_datasets * 2)) * 100, 2) if total_datasets > 0 else 0
            }
            
        except Exception as e:
            return {'error': str(e)}

# Global data quality assessor instance
quality_assessor = DataQualityAssessor()

