"""
Post-mortem Analysis and PDF Generation System
Provides comprehensive post-mortem analysis for vanished datasets
"""

import sqlite3
import json
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import logging
from pathlib import Path
import requests
from jinja2 import Template
try:
    import weasyprint
    WEASYPRINT_AVAILABLE = True
except ImportError:
    WEASYPRINT_AVAILABLE = False
    weasyprint = None

from io import BytesIO

logger = logging.getLogger(__name__)

class PostMortemSystem:
    """Comprehensive post-mortem analysis system"""
    
    def __init__(self, db_path: str = "datasets.db", output_dir: str = "postmortems"):
        self.db_path = db_path
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Post-mortem templates
        self.html_template = self._get_html_template()
        self.pdf_template = self._get_pdf_template()
    
    def _get_html_template(self) -> str:
        """Get HTML template for post-mortem reports"""
        return """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Dataset Post-mortem: {{ dataset_id }}</title>
    <style>
        @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@300;400;500;600&display=swap');
        
        body {
            font-family: 'IBM Plex Mono', monospace;
            margin: 0;
            padding: 20px;
            background-color: #f8f8f8;
            color: #2c2c2c;
            line-height: 1.6;
        }
        
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background-color: #ffffff;
            padding: 30px;
            border: 1px solid #e0e0e0;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        
        h1 {
            color: #1a1a1a;
            border-bottom: 2px solid #4a4a4a;
            padding-bottom: 15px;
            margin-top: 0;
            font-weight: 600;
            font-size: 28px;
        }
        
        h2 {
            color: #2c2c2c;
            border-bottom: 1px solid #d0d0d0;
            padding-bottom: 10px;
            margin-top: 30px;
            font-weight: 500;
        }
        
        h3 {
            color: #4a4a4a;
            margin-top: 25px;
            font-weight: 500;
        }
        
        .status-badge {
            display: inline-block;
            padding: 4px 12px;
            border-radius: 0;
            font-size: 12px;
            font-weight: 500;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        .status-vanished {
            background-color: #ffebee;
            color: #c62828;
            border: 1px solid #ffcdd2;
        }
        
        .status-moved {
            background-color: #fff3e0;
            color: #ef6c00;
            border: 1px solid #ffcc02;
        }
        
        .status-draft {
            background-color: #f3e5f5;
            color: #7b1fa2;
            border: 1px solid #e1bee7;
        }
        
        .metadata-table {
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
        }
        
        .metadata-table th,
        .metadata-table td {
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #e0e0e0;
        }
        
        .metadata-table th {
            background-color: #f5f5f5;
            font-weight: 500;
            color: #2c2c2c;
        }
        
        .timeline {
            margin: 20px 0;
        }
        
        .timeline-item {
            display: flex;
            margin-bottom: 15px;
            padding: 15px;
            background-color: #f9f9f9;
            border-left: 4px solid #4a4a4a;
        }
        
        .timeline-date {
            font-weight: 500;
            color: #4a4a4a;
            min-width: 120px;
        }
        
        .timeline-content {
            flex: 1;
            margin-left: 20px;
        }
        
        .change-log {
            margin: 20px 0;
        }
        
        .change-item {
            padding: 10px;
            margin-bottom: 10px;
            border-left: 3px solid #4a4a4a;
            background-color: #f9f9f9;
        }
        
        .change-item.high-severity {
            border-left-color: #d32f2f;
            background-color: #ffebee;
        }
        
        .change-item.medium-severity {
            border-left-color: #f57c00;
            background-color: #fff3e0;
        }
        
        .change-item.low-severity {
            border-left-color: #388e3c;
            background-color: #e8f5e8;
        }
        
        .severity-badge {
            display: inline-block;
            padding: 2px 8px;
            font-size: 10px;
            font-weight: 500;
            text-transform: uppercase;
            border-radius: 0;
            margin-right: 10px;
        }
        
        .severity-high {
            background-color: #d32f2f;
            color: white;
        }
        
        .severity-medium {
            background-color: #f57c00;
            color: white;
        }
        
        .severity-low {
            background-color: #388e3c;
            color: white;
        }
        
        .analysis-section {
            margin: 30px 0;
            padding: 20px;
            background-color: #f5f5f5;
            border: 1px solid #e0e0e0;
        }
        
        .recommendations {
            margin: 20px 0;
        }
        
        .recommendation-item {
            padding: 10px;
            margin-bottom: 10px;
            background-color: #e3f2fd;
            border-left: 4px solid #2196f3;
        }
        
        .archive-links {
            margin: 20px 0;
        }
        
        .archive-link {
            display: inline-block;
            padding: 8px 16px;
            margin: 5px;
            background-color: #4a4a4a;
            color: white;
            text-decoration: none;
            border-radius: 0;
            font-size: 14px;
        }
        
        .archive-link:hover {
            background-color: #2c2c2c;
        }
        
        .footer {
            margin-top: 40px;
            padding-top: 20px;
            border-top: 1px solid #e0e0e0;
            color: #666;
            font-size: 12px;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>Dataset Post-mortem Analysis</h1>
        
        <div class="status-badge status-{{ status_class }}">{{ status }}</div>
        
        <h2>Dataset Information</h2>
        <table class="metadata-table">
            <tr>
                <th>Dataset ID</th>
                <td>{{ dataset_id }}</td>
            </tr>
            <tr>
                <th>Title</th>
                <td>{{ title }}</td>
            </tr>
            <tr>
                <th>Agency</th>
                <td>{{ agency }}</td>
            </tr>
            <tr>
                <th>Publisher</th>
                <td>{{ publisher }}</td>
            </tr>
            <tr>
                <th>Last Seen</th>
                <td>{{ last_seen }}</td>
            </tr>
            <tr>
                <th>First Seen</th>
                <td>{{ first_seen }}</td>
            </tr>
            <tr>
                <th>Total Snapshots</th>
                <td>{{ snapshot_count }}</td>
            </tr>
            <tr>
                <th>Volatility Score</th>
                <td>{{ volatility_score }}</td>
            </tr>
        </table>
        
        <h2>Timeline Analysis</h2>
        <div class="timeline">
            {% for event in timeline %}
            <div class="timeline-item">
                <div class="timeline-date">{{ event.date }}</div>
                <div class="timeline-content">
                    <strong>{{ event.event }}</strong><br>
                    {{ event.description }}
                </div>
            </div>
            {% endfor %}
        </div>
        
        <h2>Change Analysis</h2>
        <div class="change-log">
            {% for change in changes %}
            <div class="change-item {{ change.severity }}-severity">
                <span class="severity-badge severity-{{ change.severity }}">{{ change.severity }}</span>
                <strong>{{ change.date }}</strong> - {{ change.description }}
            </div>
            {% endfor %}
        </div>
        
        <h2>Root Cause Analysis</h2>
        <div class="analysis-section">
            <h3>Suspected Cause</h3>
            <p>{{ suspected_cause }}</p>
            
            <h3>Evidence</h3>
            <ul>
                {% for evidence in evidence_list %}
                <li>{{ evidence }}</li>
                {% endfor %}
            </ul>
        </div>
        
        <h2>Archive Links</h2>
        <div class="archive-links">
            {% for link in archive_links %}
            <a href="{{ link.url }}" class="archive-link" target="_blank">{{ link.name }}</a>
            {% endfor %}
        </div>
        
        <h2>Recommendations</h2>
        <div class="recommendations">
            {% for rec in recommendations %}
            <div class="recommendation-item">
                <strong>{{ rec.title }}</strong><br>
                {{ rec.description }}
            </div>
            {% endfor %}
        </div>
        
        <div class="footer">
            <p>Generated on {{ generated_date }} by Dataset State Historian</p>
            <p>Report ID: {{ report_id }}</p>
        </div>
    </div>
</body>
</html>
        """
    
    def _get_pdf_template(self) -> str:
        """Get PDF-specific template"""
        return self.html_template  # Same template, different CSS for PDF
    
    def generate_postmortem(self, dataset_id: str) -> Dict:
        """Generate comprehensive post-mortem analysis"""
        try:
            # Get dataset information
            dataset_info = self._get_dataset_info(dataset_id)
            if not dataset_info:
                return {'error': 'Dataset not found'}
            
            # Get timeline and changes
            timeline = self._get_dataset_timeline(dataset_id)
            changes = self._get_dataset_changes(dataset_id)
            
            # Analyze root cause
            root_cause_analysis = self._analyze_root_cause(dataset_id, timeline, changes)
            
            # Find archive links
            archive_links = self._find_archive_links(dataset_id, dataset_info)
            
            # Generate recommendations
            recommendations = self._generate_recommendations(dataset_id, root_cause_analysis)
            
            # Create post-mortem data
            postmortem_data = {
                'dataset_id': dataset_id,
                'title': dataset_info.get('title', 'Unknown'),
                'agency': dataset_info.get('agency', 'Unknown'),
                'publisher': dataset_info.get('publisher', 'Unknown'),
                'last_seen': dataset_info.get('last_seen', 'Unknown'),
                'first_seen': dataset_info.get('first_seen', 'Unknown'),
                'snapshot_count': len(timeline),
                'volatility_score': dataset_info.get('volatility_score', 0.0),
                'status': root_cause_analysis['status'],
                'status_class': root_cause_analysis['status_class'],
                'timeline': timeline,
                'changes': changes,
                'suspected_cause': root_cause_analysis['suspected_cause'],
                'evidence_list': root_cause_analysis['evidence'],
                'archive_links': archive_links,
                'recommendations': recommendations,
                'generated_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'report_id': hashlib.md5(f"{dataset_id}_{datetime.now()}".encode()).hexdigest()[:8]
            }
            
            return postmortem_data
            
        except Exception as e:
            logger.error(f"Error generating post-mortem for {dataset_id}: {e}")
            return {'error': str(e)}
    
    def _get_dataset_info(self, dataset_id: str) -> Optional[Dict]:
        """Get basic dataset information"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get latest snapshot
        cursor.execute('''
            SELECT title, agency, publisher, license, landing_page, 
                   modified, snapshot_date, created_at
            FROM historian_snapshots
            WHERE dataset_id = ?
            ORDER BY snapshot_date DESC
            LIMIT 1
        ''', (dataset_id,))
        
        row = cursor.fetchone()
        if not row:
            conn.close()
            return None
        
        # Get first seen date
        cursor.execute('''
            SELECT MIN(snapshot_date) as first_seen
            FROM historian_snapshots
            WHERE dataset_id = ?
        ''', (dataset_id,))
        
        first_seen = cursor.fetchone()[0]
        
        # Get volatility score
        cursor.execute('''
            SELECT AVG(volatility_score) as avg_volatility
            FROM historian_diffs
            WHERE dataset_id = ?
        ''', (dataset_id,))
        
        volatility = cursor.fetchone()[0] or 0.0
        
        conn.close()
        
        return {
            'title': row[0],
            'agency': row[1],
            'publisher': row[2],
            'license': row[3],
            'landing_page': row[4],
            'modified': row[5],
            'last_seen': row[6],
            'first_seen': first_seen,
            'volatility_score': volatility
        }
    
    def _get_dataset_timeline(self, dataset_id: str) -> List[Dict]:
        """Get dataset timeline for post-mortem"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT snapshot_date, title, agency, created_at
            FROM historian_snapshots
            WHERE dataset_id = ?
            ORDER BY snapshot_date ASC
        ''', (dataset_id,))
        
        timeline = []
        for row in cursor.fetchall():
            timeline.append({
                'date': row[0],
                'title': row[1],
                'agency': row[2],
                'created_at': row[3],
                'event': 'Dataset Snapshot',
                'description': f"Captured snapshot of {row[1]}"
            })
        
        conn.close()
        return timeline
    
    def _get_dataset_changes(self, dataset_id: str) -> List[Dict]:
        """Get dataset changes for post-mortem"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT from_date, to_date, metadata_changes, schema_changes,
                   content_changes, volatility_score, change_events
            FROM historian_diffs
            WHERE dataset_id = ?
            ORDER BY from_date ASC
        ''', (dataset_id,))
        
        changes = []
        for row in cursor.fetchall():
            from_date, to_date, metadata_changes, schema_changes, content_changes, volatility, events = row
            
            # Parse changes
            if metadata_changes:
                meta_changes = json.loads(metadata_changes)
                for change in meta_changes:
                    changes.append({
                        'date': to_date,
                        'description': f"{change.get('field', 'Field')} changed from '{change.get('old_value', 'N/A')}' to '{change.get('new_value', 'N/A')}'",
                        'severity': self._calculate_change_severity(change, volatility)
                    })
            
            if schema_changes:
                schema_changes_list = json.loads(schema_changes)
                for change in schema_changes_list:
                    changes.append({
                        'date': to_date,
                        'description': f"Schema change: {change.get('change_type', 'Unknown')} - {change.get('column_name', 'Column')}",
                        'severity': self._calculate_change_severity(change, volatility)
                    })
            
            if events:
                event_list = json.loads(events)
                for event in event_list:
                    changes.append({
                        'date': to_date,
                        'description': event.get('description', 'Unknown event'),
                        'severity': event.get('severity', 'low')
                    })
        
        # Sort by date (most recent first)
        changes.sort(key=lambda x: x['date'], reverse=True)
        
        conn.close()
        return changes
    
    def _analyze_root_cause(self, dataset_id: str, timeline: List[Dict], changes: List[Dict]) -> Dict:
        """Analyze root cause of dataset disappearance"""
        # Check if dataset is actually missing or just moved
        status = "vanished"
        status_class = "vanished"
        suspected_cause = "Unknown"
        evidence = []
        
        # Analyze timeline patterns
        if len(timeline) < 2:
            suspected_cause = "Insufficient data for analysis"
            evidence.append("Only one snapshot available")
        else:
            # Check for recent changes
            recent_changes = [c for c in changes if c['date'] >= (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')]
            
            if recent_changes:
                # Check for specific change patterns
                license_changes = [c for c in recent_changes if 'license' in c['description'].lower()]
                schema_changes = [c for c in recent_changes if 'schema' in c['description'].lower()]
                url_changes = [c for c in recent_changes if 'url' in c['description'].lower() or 'landing_page' in c['description'].lower()]
                
                if url_changes:
                    suspected_cause = "URL change or domain migration"
                    status = "moved"
                    status_class = "moved"
                    evidence.append("Recent URL or landing page changes detected")
                
                elif license_changes:
                    suspected_cause = "License change or policy update"
                    evidence.append("Recent license changes detected")
                
                elif schema_changes:
                    suspected_cause = "Data structure changes"
                    evidence.append("Recent schema changes detected")
                
                else:
                    suspected_cause = "Gradual degradation or policy change"
                    evidence.append("Multiple recent changes detected")
            else:
                suspected_cause = "Sudden removal or system failure"
                evidence.append("No recent changes before disappearance")
        
        # Check volatility
        high_volatility_changes = [c for c in changes if c['severity'] == 'high']
        if high_volatility_changes:
            evidence.append(f"High volatility detected: {len(high_volatility_changes)} high-severity changes")
        
        return {
            'status': status,
            'status_class': status_class,
            'suspected_cause': suspected_cause,
            'evidence': evidence
        }
    
    def _find_archive_links(self, dataset_id: str, dataset_info: Dict) -> List[Dict]:
        """Find archive links for the dataset"""
        links = []
        
        # Wayback Machine link
        if dataset_info.get('landing_page'):
            wayback_url = f"https://web.archive.org/web/*/{dataset_info['landing_page']}"
            links.append({
                'name': 'Wayback Machine',
                'url': wayback_url
            })
        
        # LIL Archive link (if available)
        lil_url = f"https://lil.law.harvard.edu/data-gov-archive/dataset/{dataset_id}"
        links.append({
            'name': 'LIL Archive',
            'url': lil_url
        })
        
        # Original dataset page
        if dataset_info.get('landing_page'):
            links.append({
                'name': 'Original Dataset',
                'url': dataset_info['landing_page']
            })
        
        return links
    
    def _generate_recommendations(self, dataset_id: str, root_cause: Dict) -> List[Dict]:
        """Generate recommendations based on analysis"""
        recommendations = []
        
        if root_cause['status'] == 'moved':
            recommendations.append({
                'title': 'Check for URL Changes',
                'description': 'The dataset may have been moved to a new URL. Check agency websites and data portals for updated locations.'
            })
        
        if 'license' in root_cause['suspected_cause'].lower():
            recommendations.append({
                'title': 'Review License Changes',
                'description': 'Recent license changes may indicate policy updates. Contact the publishing agency for clarification.'
            })
        
        if 'schema' in root_cause['suspected_cause'].lower():
            recommendations.append({
                'title': 'Check Data Format Changes',
                'description': 'The dataset structure may have changed. Look for updated versions with different file formats.'
            })
        
        # General recommendations
        recommendations.extend([
            {
                'title': 'Contact Publishing Agency',
                'description': 'Reach out to the agency that originally published this dataset for information about its current status.'
            },
            {
                'title': 'Monitor for Reappearance',
                'description': 'Set up monitoring to detect if the dataset becomes available again in the future.'
            },
            {
                'title': 'Document Findings',
                'description': 'Keep records of this analysis for future reference and to help identify similar patterns.'
            }
        ])
        
        return recommendations
    
    def _calculate_change_severity(self, change: Dict, volatility: float) -> str:
        """Calculate change severity"""
        change_type = change.get('change_type', 'unknown')
        
        if change_type in ['license_change', 'column_removed']:
            return 'high'
        elif change_type in ['schema_change', 'content_change']:
            return 'medium'
        elif volatility > 0.5:
            return 'high'
        elif volatility > 0.2:
            return 'medium'
        else:
            return 'low'
    
    def generate_html_report(self, postmortem_data: Dict) -> str:
        """Generate HTML post-mortem report"""
        try:
            template = Template(self.html_template)
            html_content = template.render(**postmortem_data)
            
            # Save HTML file
            report_id = postmortem_data['report_id']
            html_file = self.output_dir / f"postmortem_{report_id}.html"
            
            with open(html_file, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            logger.info(f"HTML post-mortem report saved: {html_file}")
            return str(html_file)
            
        except Exception as e:
            logger.error(f"Error generating HTML report: {e}")
            return None
    
    def generate_pdf_report(self, postmortem_data: Dict) -> str:
        """Generate PDF post-mortem report"""
        if not WEASYPRINT_AVAILABLE:
            logger.warning("WeasyPrint not available, PDF generation disabled")
            return None
            
        try:
            # Generate HTML first
            html_file = self.generate_html_report(postmortem_data)
            if not html_file:
                return None
            
            # Convert to PDF
            report_id = postmortem_data['report_id']
            pdf_file = self.output_dir / f"postmortem_{report_id}.pdf"
            
            # Use weasyprint to convert HTML to PDF
            with open(html_file, 'r', encoding='utf-8') as f:
                html_content = f.read()
            
            weasyprint.HTML(string=html_content).write_pdf(str(pdf_file))
            
            logger.info(f"PDF post-mortem report saved: {pdf_file}")
            return str(pdf_file)
            
        except Exception as e:
            logger.error(f"Error generating PDF report: {e}")
            return None
    
    def generate_batch_postmortems(self, dataset_ids: List[str]) -> Dict:
        """Generate post-mortem reports for multiple datasets"""
        results = {
            'successful': [],
            'failed': [],
            'reports': []
        }
        
        for dataset_id in dataset_ids:
            try:
                # Generate post-mortem data
                postmortem_data = self.generate_postmortem(dataset_id)
                if 'error' in postmortem_data:
                    results['failed'].append({
                        'dataset_id': dataset_id,
                        'error': postmortem_data['error']
                    })
                    continue
                
                # Generate HTML report
                html_file = self.generate_html_report(postmortem_data)
                
                # Generate PDF report
                pdf_file = self.generate_pdf_report(postmortem_data)
                
                results['successful'].append(dataset_id)
                results['reports'].append({
                    'dataset_id': dataset_id,
                    'html_file': html_file,
                    'pdf_file': pdf_file,
                    'report_id': postmortem_data['report_id']
                })
                
            except Exception as e:
                logger.error(f"Error generating post-mortem for {dataset_id}: {e}")
                results['failed'].append({
                    'dataset_id': dataset_id,
                    'error': str(e)
                })
        
        return results
