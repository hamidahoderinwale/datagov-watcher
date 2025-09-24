"""
State Explorer: Generate human-readable reports of dataset state changes
"""

import json
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class StateExplorer:
    def __init__(self, db_path: str = "datasets.db", base_dir: str = "dataset_states"):
        self.db_path = db_path
        self.base_dir = Path(base_dir)
        self.reports_dir = Path("state_reports")
        self.reports_dir.mkdir(exist_ok=True)
    
    def generate_dataset_report(self, dataset_id: str) -> str:
        """Generate comprehensive report for a single dataset"""
        logger.info(f"Generating state report for dataset {dataset_id}")
        
        # Get timeline and diffs
        timeline = self._get_dataset_timeline(dataset_id)
        diffs = self._get_state_diffs(dataset_id)
        volatility = self._calculate_volatility_score(dataset_id)
        
        # Get latest snapshot
        latest_snapshot = self._get_latest_snapshot(dataset_id)
        
        # Generate HTML report
        html_content = self._generate_html_report(dataset_id, timeline, diffs, volatility, latest_snapshot)
        
        # Save report
        report_file = self.reports_dir / f"{dataset_id}_state_report.html"
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"State report saved: {report_file}")
        return str(report_file)
    
    def generate_summary_report(self) -> str:
        """Generate summary report for all datasets"""
        logger.info("Generating summary state report")
        
        # Get all datasets with state data
        datasets = self._get_all_datasets_with_states()
        
        # Calculate summary statistics
        summary_stats = self._calculate_summary_stats(datasets)
        
        # Generate HTML summary
        html_content = self._generate_summary_html(summary_stats, datasets)
        
        # Save report
        report_file = self.reports_dir / "state_summary_report.html"
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"Summary report saved: {report_file}")
        return str(report_file)
    
    def _get_dataset_timeline(self, dataset_id: str) -> List[Dict]:
        """Get timeline data for dataset"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT snapshot_date, row_count, column_count, file_size, created_at
            FROM dataset_states 
            WHERE dataset_id = ?
            ORDER BY snapshot_date ASC
        ''', (dataset_id,))
        
        columns = [description[0] for description in cursor.description]
        timeline = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        conn.close()
        return timeline
    
    def _get_state_diffs(self, dataset_id: str) -> List[Dict]:
        """Get state diffs for dataset"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT from_date, to_date, diff_type, diff_data, created_at
            FROM state_diffs 
            WHERE dataset_id = ?
            ORDER BY from_date ASC
        ''', (dataset_id,))
        
        columns = [description[0] for description in cursor.description]
        diffs = []
        
        for row in cursor.fetchall():
            diff = dict(zip(columns, row))
            diff['diff_data'] = json.loads(diff['diff_data'])
            diffs.append(diff)
        
        conn.close()
        return diffs
    
    def _calculate_volatility_score(self, dataset_id: str) -> Dict:
        """Calculate volatility score"""
        timeline = self._get_dataset_timeline(dataset_id)
        diffs = self._get_state_diffs(dataset_id)
        
        if not timeline:
            return {'volatility_score': 0, 'change_count': 0, 'age_days': 0}
        
        # Calculate age
        first_date = datetime.strptime(timeline[0]['snapshot_date'], '%Y-%m-%d')
        last_date = datetime.strptime(timeline[-1]['snapshot_date'], '%Y-%m-%d')
        age_days = (last_date - first_date).days
        
        # Count changes
        change_count = len(diffs)
        
        # Calculate volatility score (changes per year)
        volatility_score = change_count / (age_days / 365.25) if age_days > 0 else 0
        
        return {
            'volatility_score': volatility_score,
            'change_count': change_count,
            'age_days': age_days,
            'snapshots': len(timeline)
        }
    
    def _get_latest_snapshot(self, dataset_id: str) -> Optional[Dict]:
        """Get latest snapshot data"""
        timeline_dir = self.base_dir / dataset_id
        if not timeline_dir.exists():
            return None
        
        # Find latest snapshot directory
        snapshot_dirs = [d for d in timeline_dir.iterdir() if d.is_dir()]
        if not snapshot_dirs:
            return None
        
        latest_dir = max(snapshot_dirs, key=lambda x: x.name)
        metadata_file = latest_dir / 'metadata.json'
        
        if metadata_file.exists():
            with open(metadata_file, 'r') as f:
                return json.load(f)
        
        return None
    
    def _get_all_datasets_with_states(self) -> List[Dict]:
        """Get all datasets that have state data"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT dataset_id, COUNT(*) as snapshot_count, 
                   MIN(snapshot_date) as first_seen, 
                   MAX(snapshot_date) as last_seen
            FROM dataset_states 
            GROUP BY dataset_id
            ORDER BY last_seen DESC
        ''')
        
        columns = [description[0] for description in cursor.description]
        datasets = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        conn.close()
        return datasets
    
    def _calculate_summary_stats(self, datasets: List[Dict]) -> Dict:
        """Calculate summary statistics"""
        total_datasets = len(datasets)
        total_snapshots = sum(d['snapshot_count'] for d in datasets)
        
        # Calculate volatility scores
        volatility_scores = []
        for dataset in datasets:
            volatility = self._calculate_volatility_score(dataset['dataset_id'])
            volatility_scores.append(volatility['volatility_score'])
        
        # Find most/least volatile datasets
        if volatility_scores:
            max_volatility = max(volatility_scores)
            min_volatility = min(volatility_scores)
            avg_volatility = sum(volatility_scores) / len(volatility_scores)
        else:
            max_volatility = min_volatility = avg_volatility = 0
        
        return {
            'total_datasets': total_datasets,
            'total_snapshots': total_snapshots,
            'avg_volatility': avg_volatility,
            'max_volatility': max_volatility,
            'min_volatility': min_volatility,
            'datasets': datasets
        }
    
    def _generate_html_report(self, dataset_id: str, timeline: List[Dict], diffs: List[Dict], 
                            volatility: Dict, latest_snapshot: Optional[Dict]) -> str:
        """Generate HTML report for single dataset"""
        
        # Timeline chart data
        timeline_data = []
        for point in timeline:
            timeline_data.append({
                'date': point['snapshot_date'],
                'rows': point['row_count'] or 0,
                'columns': point['column_count'] or 0,
                'size': point['file_size'] or 0
            })
        
        # Changes summary
        schema_changes = [d for d in diffs if d['diff_type'] == 'schema']
        metadata_changes = [d for d in diffs if d['diff_type'] == 'metadata']
        content_changes = [d for d in diffs if d['diff_type'] == 'content']
        
        html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Dataset State Report - {dataset_id}</title>
    <style>
        @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@300;400;500;600&display=swap');
        
        body {{
            font-family: 'IBM Plex Mono', monospace;
            margin: 20px;
            background-color: #f8f8f8;
            color: #2c2c2c;
            line-height: 1.6;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background-color: #ffffff;
            padding: 30px;
            border: 1px solid #e0e0e0;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        h1 {{
            color: #1a1a1a;
            border-bottom: 2px solid #4a4a4a;
            padding-bottom: 15px;
            margin-top: 0;
            font-weight: 600;
            font-size: 28px;
        }}
        h2 {{
            color: #3a3a3a;
            font-weight: 500;
            font-size: 20px;
            margin-top: 30px;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin: 20px 0;
        }}
        .stat-card {{
            background-color: #f5f5f5;
            padding: 20px;
            border: 1px solid #d0d0d0;
        }}
        .stat-value {{
            font-size: 24px;
            font-weight: 600;
            color: #1a1a1a;
        }}
        .stat-label {{
            font-size: 14px;
            color: #6a6a6a;
            margin-top: 5px;
        }}
        .timeline-chart {{
            background-color: #fafafa;
            padding: 20px;
            border: 1px solid #e0e0e0;
            margin: 20px 0;
        }}
        .change-log {{
            margin: 20px 0;
        }}
        .change-item {{
            background-color: #f9f9f9;
            padding: 15px;
            margin: 10px 0;
            border-left: 4px solid #4a4a4a;
        }}
        .change-date {{
            font-weight: 500;
            color: #4a4a4a;
        }}
        .change-details {{
            margin-top: 10px;
            font-size: 14px;
        }}
        .volatility-high {{ color: #d32f2f; }}
        .volatility-medium {{ color: #f57c00; }}
        .volatility-low {{ color: #4caf50; }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
        }}
        th, td {{
            border: 1px solid #d0d0d0;
            padding: 12px;
            text-align: left;
        }}
        th {{
            background-color: #f0f0f0;
            font-weight: 500;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Dataset State Report: {dataset_id}</h1>
        
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-value">{volatility['snapshots']}</div>
                <div class="stat-label">Snapshots</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{volatility['change_count']}</div>
                <div class="stat-label">Changes</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{volatility['age_days']}</div>
                <div class="stat-label">Age (days)</div>
            </div>
            <div class="stat-card">
                <div class="stat-value volatility-{'high' if volatility['volatility_score'] > 10 else 'medium' if volatility['volatility_score'] > 5 else 'low'}">
                    {volatility['volatility_score']:.1f}
                </div>
                <div class="stat-label">Volatility Score</div>
            </div>
        </div>
        
        <h2>Timeline Overview</h2>
        <div class="timeline-chart">
            <p>Dataset has been tracked for {volatility['age_days']} days with {volatility['snapshots']} snapshots.</p>
            <p>Most recent snapshot: {timeline[-1]['snapshot_date'] if timeline else 'None'}</p>
        </div>
        
        <h2>Change History</h2>
        <div class="change-log">
"""
        
        # Add change items
        for diff in diffs:
            html += f"""
            <div class="change-item">
                <div class="change-date">{diff['from_date']} → {diff['to_date']} ({diff['diff_type']})</div>
                <div class="change-details">
"""
            
            if diff['diff_type'] == 'schema':
                for change in diff['diff_data'].get('column_changes', []):
                    html += f"<div>• {change}</div>"
                for change in diff['diff_data'].get('type_changes', []):
                    html += f"<div>• {change}</div>"
            elif diff['diff_type'] == 'metadata':
                for change in diff['diff_data'].get('changes', []):
                    html += f"<div>• {change}</div>"
            elif diff['diff_type'] == 'content':
                for change in diff['diff_data'].get('changes', []):
                    html += f"<div>• {change}</div>"
            
            html += """
                </div>
            </div>
"""
        
        html += """
        </div>
        
        <h2>Latest State</h2>
"""
        
        if latest_snapshot:
            html += f"""
        <table>
            <tr><th>Field</th><th>Value</th></tr>
            <tr><td>Title</td><td>{latest_snapshot.get('title', 'N/A')}</td></tr>
            <tr><td>Agency</td><td>{latest_snapshot.get('agency', 'N/A')}</td></tr>
            <tr><td>URL</td><td><a href="{latest_snapshot.get('url', '#')}" target="_blank">{latest_snapshot.get('url', 'N/A')}</a></td></tr>
            <tr><td>Availability</td><td>{latest_snapshot.get('availability', 'N/A')}</td></tr>
            <tr><td>Row Count</td><td>{latest_snapshot.get('content_stats', {}).get('row_count', 'N/A')}</td></tr>
            <tr><td>Column Count</td><td>{latest_snapshot.get('content_stats', {}).get('column_count', 'N/A')}</td></tr>
        </table>
"""
        else:
            html += "<p>No snapshot data available.</p>"
        
        html += f"""
        <div style="margin-top: 40px; padding-top: 20px; border-top: 1px solid #e0e0e0; color: #6a6a6a; font-size: 14px;">
            Report generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        </div>
    </div>
</body>
</html>
"""
        
        return html
    
    def _generate_summary_html(self, summary_stats: Dict, datasets: List[Dict]) -> str:
        """Generate summary HTML report"""
        
        html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Dataset State Summary Report</title>
    <style>
        @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@300;400;500;600&display=swap');
        
        body {{
            font-family: 'IBM Plex Mono', monospace;
            margin: 20px;
            background-color: #f8f8f8;
            color: #2c2c2c;
            line-height: 1.6;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background-color: #ffffff;
            padding: 30px;
            border: 1px solid #e0e0e0;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        h1 {{
            color: #1a1a1a;
            border-bottom: 2px solid #4a4a4a;
            padding-bottom: 15px;
            margin-top: 0;
            font-weight: 600;
            font-size: 28px;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin: 20px 0;
        }}
        .stat-card {{
            background-color: #f5f5f5;
            padding: 20px;
            border: 1px solid #d0d0d0;
        }}
        .stat-value {{
            font-size: 24px;
            font-weight: 600;
            color: #1a1a1a;
        }}
        .stat-label {{
            font-size: 14px;
            color: #6a6a6a;
            margin-top: 5px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
        }}
        th, td {{
            border: 1px solid #d0d0d0;
            padding: 12px;
            text-align: left;
        }}
        th {{
            background-color: #f0f0f0;
            font-weight: 500;
        }}
        .volatility-high {{ color: #d32f2f; }}
        .volatility-medium {{ color: #f57c00; }}
        .volatility-low {{ color: #4caf50; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Dataset State Summary Report</h1>
        
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-value">{summary_stats['total_datasets']}</div>
                <div class="stat-label">Total Datasets</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{summary_stats['total_snapshots']}</div>
                <div class="stat-label">Total Snapshots</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{summary_stats['avg_volatility']:.1f}</div>
                <div class="stat-label">Avg Volatility</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{summary_stats['max_volatility']:.1f}</div>
                <div class="stat-label">Max Volatility</div>
            </div>
        </div>
        
        <h2>Dataset Overview</h2>
        <table>
            <thead>
                <tr>
                    <th>Dataset ID</th>
                    <th>Snapshots</th>
                    <th>First Seen</th>
                    <th>Last Seen</th>
                    <th>Volatility</th>
                </tr>
            </thead>
            <tbody>
"""
        
        for dataset in datasets[:50]:  # Show top 50
            volatility = self._calculate_volatility_score(dataset['dataset_id'])
            volatility_class = 'high' if volatility['volatility_score'] > 10 else 'medium' if volatility['volatility_score'] > 5 else 'low'
            
            html += f"""
                <tr>
                    <td>{dataset['dataset_id']}</td>
                    <td>{dataset['snapshot_count']}</td>
                    <td>{dataset['first_seen']}</td>
                    <td>{dataset['last_seen']}</td>
                    <td class="volatility-{volatility_class}">{volatility['volatility_score']:.1f}</td>
                </tr>
"""
        
        html += f"""
            </tbody>
        </table>
        
        <div style="margin-top: 40px; padding-top: 20px; border-top: 1px solid #e0e0e0; color: #6a6a6a; font-size: 14px;">
            Report generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        </div>
    </div>
</body>
</html>
"""
        
        return html
