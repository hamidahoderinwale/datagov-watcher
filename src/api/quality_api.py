"""
Data Quality API - Data quality assessment and validation endpoints
"""

from flask import Blueprint, request, jsonify
from ..quality.data_quality import quality_assessor
from ..auth.authentication import require_auth, require_permission

quality_bp = Blueprint('quality', __name__, url_prefix='/api/quality')

@quality_bp.route('/assess/<dataset_id>')
@require_auth
@require_permission('read_analytics')
def assess_dataset_quality(dataset_id):
    """Assess data quality for a specific dataset"""
    try:
        quality_report = quality_assessor.assess_dataset_quality(dataset_id)
        
        if 'error' in quality_report:
            return jsonify(quality_report), 400
        
        return jsonify(quality_report)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@quality_bp.route('/summary')
@require_auth
@require_permission('read_analytics')
def get_quality_summary():
    """Get overall data quality summary"""
    try:
        summary = quality_assessor.get_quality_summary()
        
        if 'error' in summary:
            return jsonify(summary), 400
        
        return jsonify(summary)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@quality_bp.route('/metrics')
@require_auth
@require_permission('read_analytics')
def get_quality_metrics():
    """Get detailed quality metrics"""
    try:
        import sqlite3
        
        conn = sqlite3.connect('datasets.db')
        cursor = conn.cursor()
        
        # Get quality metrics by agency
        cursor.execute('''
            SELECT d.agency, 
                   COUNT(*) as total_datasets,
                   COUNT(CASE WHEN ds.availability = 'available' THEN 1 END) as available_datasets,
                   COUNT(CASE WHEN d.title IS NOT NULL AND d.title != '' THEN 1 END) as with_title,
                   COUNT(CASE WHEN d.description IS NOT NULL AND d.description != '' THEN 1 END) as with_description,
                   COUNT(CASE WHEN ds.row_count IS NOT NULL AND ds.row_count > 0 THEN 1 END) as with_dimensions
            FROM datasets d
            LEFT JOIN dataset_states ds ON d.id = ds.dataset_id
            WHERE ds.snapshot_date = (
                SELECT MAX(snapshot_date) 
                FROM dataset_states ds2 
                WHERE ds2.dataset_id = d.id
            )
            GROUP BY d.agency
            ORDER BY total_datasets DESC
            LIMIT 20
        ''')
        
        agency_metrics = []
        for row in cursor.fetchall():
            agency, total, available, with_title, with_description, with_dimensions = row
            
            agency_metrics.append({
                'agency': agency,
                'total_datasets': total,
                'available_datasets': available,
                'with_title': with_title,
                'with_description': with_description,
                'with_dimensions': with_dimensions,
                'availability_rate': round((available / total) * 100, 2) if total > 0 else 0,
                'completeness_rate': round(((with_title + with_description + with_dimensions) / (total * 3)) * 100, 2) if total > 0 else 0
            })
        
        # Get quality trends over time
        cursor.execute('''
            SELECT DATE(snapshot_date) as date,
                   COUNT(*) as total_snapshots,
                   COUNT(CASE WHEN availability = 'available' THEN 1 END) as available_snapshots
            FROM dataset_states
            WHERE snapshot_date >= date('now', '-30 days')
            GROUP BY DATE(snapshot_date)
            ORDER BY date
        ''')
        
        quality_trends = []
        for row in cursor.fetchall():
            date, total, available = row
            quality_trends.append({
                'date': date,
                'total_snapshots': total,
                'available_snapshots': available,
                'availability_rate': round((available / total) * 100, 2) if total > 0 else 0
            })
        
        # Get format distribution
        cursor.execute('''
            SELECT resource_format, COUNT(*) as count
            FROM dataset_states
            WHERE resource_format IS NOT NULL
            AND snapshot_date = (
                SELECT MAX(snapshot_date) 
                FROM dataset_states ds2 
                WHERE ds2.dataset_id = dataset_states.dataset_id
            )
            GROUP BY resource_format
            ORDER BY count DESC
        ''')
        
        format_distribution = []
        for row in cursor.fetchall():
            format_type, count = row
            format_distribution.append({
                'format': format_type,
                'count': count
            })
        
        conn.close()
        
        return jsonify({
            'agency_metrics': agency_metrics,
            'quality_trends': quality_trends,
            'format_distribution': format_distribution,
            'summary': quality_assessor.get_quality_summary()
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@quality_bp.route('/issues')
@require_auth
@require_permission('read_analytics')
def get_quality_issues():
    """Get data quality issues and recommendations"""
    try:
        import sqlite3
        
        conn = sqlite3.connect('datasets.db')
        cursor = conn.cursor()
        
        # Get datasets with missing metadata
        cursor.execute('''
            SELECT d.id, d.title, d.agency, d.url, d.description,
                   ds.availability, ds.row_count, ds.column_count
            FROM datasets d
            LEFT JOIN dataset_states ds ON d.id = ds.dataset_id
            WHERE ds.snapshot_date = (
                SELECT MAX(snapshot_date) 
                FROM dataset_states ds2 
                WHERE ds2.dataset_id = d.id
            )
            AND (
                d.title IS NULL OR d.title = '' OR
                d.agency IS NULL OR d.agency = '' OR
                d.url IS NULL OR d.url = '' OR
                d.description IS NULL OR d.description = '' OR
                ds.row_count IS NULL OR ds.row_count = 0 OR
                ds.column_count IS NULL OR ds.column_count = 0
            )
            ORDER BY d.agency, d.title
            LIMIT 50
        ''')
        
        quality_issues = []
        for row in cursor.fetchall():
            dataset_id, title, agency, url, description, availability, row_count, column_count = row
            
            issues = []
            if not title or title.strip() == '':
                issues.append('Missing title')
            if not agency or agency.strip() == '':
                issues.append('Missing agency')
            if not url or url.strip() == '':
                issues.append('Missing URL')
            if not description or description.strip() == '':
                issues.append('Missing description')
            if not row_count or row_count == 0:
                issues.append('Missing row count')
            if not column_count or column_count == 0:
                issues.append('Missing column count')
            if availability != 'available':
                issues.append('Dataset not available')
            
            quality_issues.append({
                'dataset_id': dataset_id,
                'title': title or 'Untitled',
                'agency': agency or 'Unknown Agency',
                'url': url or 'No URL',
                'issues': issues,
                'issue_count': len(issues),
                'severity': 'high' if len(issues) >= 4 else 'medium' if len(issues) >= 2 else 'low'
            })
        
        conn.close()
        
        return jsonify({
            'quality_issues': quality_issues,
            'total_issues': len(quality_issues),
            'high_severity': len([issue for issue in quality_issues if issue['severity'] == 'high']),
            'medium_severity': len([issue for issue in quality_issues if issue['severity'] == 'medium']),
            'low_severity': len([issue for issue in quality_issues if issue['severity'] == 'low'])
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@quality_bp.route('/recommendations')
@require_auth
@require_permission('read_analytics')
def get_quality_recommendations():
    """Get data quality improvement recommendations"""
    try:
        recommendations = [
            {
                'category': 'Metadata Completeness',
                'priority': 'high',
                'description': 'Add missing metadata fields to improve data discoverability',
                'action': 'Update dataset records with complete title, description, and agency information',
                'impact': 'Improves search and filtering capabilities'
            },
            {
                'category': 'Data Dimensions',
                'priority': 'high',
                'description': 'Compute and store row/column counts for all datasets',
                'action': 'Run dimension computation process for datasets missing size information',
                'impact': 'Enables better data analysis and quality assessment'
            },
            {
                'category': 'URL Validation',
                'priority': 'medium',
                'description': 'Validate and standardize dataset URLs',
                'action': 'Check URL accessibility and format consistency',
                'impact': 'Ensures data accessibility and reliability'
            },
            {
                'category': 'Agency Standardization',
                'priority': 'medium',
                'description': 'Standardize agency names and formats',
                'action': 'Implement agency name normalization and validation',
                'impact': 'Improves data organization and filtering'
            },
            {
                'category': 'Schema Information',
                'priority': 'low',
                'description': 'Extract and store detailed schema information',
                'action': 'Parse dataset schemas and store column names and data types',
                'impact': 'Enables advanced data analysis and validation'
            },
            {
                'category': 'Data Freshness',
                'priority': 'medium',
                'description': 'Implement regular data freshness monitoring',
                'action': 'Set up automated checks for dataset updates and changes',
                'impact': 'Ensures data timeliness and relevance'
            }
        ]
        
        return jsonify({
            'recommendations': recommendations,
            'total_recommendations': len(recommendations),
            'high_priority': len([r for r in recommendations if r['priority'] == 'high']),
            'medium_priority': len([r for r in recommendations if r['priority'] == 'medium']),
            'low_priority': len([r for r in recommendations if r['priority'] == 'low'])
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@quality_bp.route('/export')
@require_auth
@require_permission('export_data')
def export_quality_report():
    """Export comprehensive quality report"""
    try:
        format_type = request.args.get('format', 'json')
        
        # Get quality summary
        summary = quality_assessor.get_quality_summary()
        
        # Get quality metrics
        metrics_response = get_quality_metrics()
        metrics_data = metrics_response[0].get_json() if metrics_response[1] == 200 else {}
        
        # Get quality issues
        issues_response = get_quality_issues()
        issues_data = issues_response[0].get_json() if issues_response[1] == 200 else {}
        
        # Get recommendations
        recommendations_response = get_quality_recommendations()
        recommendations_data = recommendations_response[0].get_json() if recommendations_response[1] == 200 else {}
        
        # Combine all data
        quality_report = {
            'export_date': datetime.now().isoformat(),
            'summary': summary,
            'metrics': metrics_data,
            'issues': issues_data,
            'recommendations': recommendations_data
        }
        
        if format_type == 'csv':
            # Convert to CSV format
            import io
            import csv
            
            output = io.StringIO()
            writer = csv.writer(output)
            
            # Write summary
            writer.writerow(['Metric', 'Value'])
            for key, value in summary.items():
                writer.writerow([key, value])
            
            output.seek(0)
            return output.getvalue(), 200, {'Content-Type': 'text/csv'}
        
        else:  # JSON format
            return jsonify(quality_report)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

