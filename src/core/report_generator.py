"""
Report generator for CSV and HTML outputs
"""
import csv
import json
from typing import List, Dict
from datetime import datetime
import os

class ReportGenerator:
    def __init__(self, output_dir: str = "reports"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
    
    def generate_csv_report(self, vanished_datasets: List[Dict], filename: str = None) -> str:
        """Generate CSV report of vanished datasets"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"vanished_datasets_{timestamp}.csv"
        
        filepath = os.path.join(self.output_dir, filename)
        
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            if vanished_datasets:
                fieldnames = vanished_datasets[0].keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(vanished_datasets)
        
        return filepath
    
    def generate_html_report(self, vanished_datasets: List[Dict], stats: Dict, filename: str = None) -> str:
        """Generate HTML report with monospace font and basic styling"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"vanished_datasets_{timestamp}.html"
        
        filepath = os.path.join(self.output_dir, filename)
        
        html_content = self._generate_html_content(vanished_datasets, stats)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        return filepath
    
    def _generate_html_content(self, vanished_datasets: List[Dict], stats: Dict) -> str:
        """Generate HTML content with IBM Plex Mono font and grayscale styling"""
        html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Data.gov Vanished Datasets Report</title>
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
        .stats {{
            background-color: #f5f5f5;
            padding: 20px;
            margin: 25px 0;
            border: 1px solid #d0d0d0;
        }}
        .stats h2 {{
            margin-top: 0;
            color: #3a3a3a;
            font-weight: 500;
            font-size: 18px;
        }}
        .stat-item {{
            margin: 8px 0;
            font-weight: 400;
            color: #2a2a2a;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 25px 0;
            font-size: 14px;
        }}
        th, td {{
            border: 1px solid #d0d0d0;
            padding: 12px;
            text-align: left;
            font-family: 'IBM Plex Mono', monospace;
        }}
        th {{
            background-color: #f0f0f0;
            font-weight: 500;
            color: #2a2a2a;
        }}
        tr:nth-child(even) {{
            background-color: #fafafa;
        }}
        tr:hover {{
            background-color: #f5f5f5;
        }}
        .status-removed {{
            color: #1a1a1a;
            font-weight: 500;
        }}
        .status-moved {{
            color: #4a4a4a;
            font-weight: 500;
        }}
        .status-draft {{
            color: #6a6a6a;
            font-weight: 500;
        }}
        .archive-link {{
            color: #4a4a4a;
            text-decoration: none;
            font-weight: 400;
        }}
        .archive-link:hover {{
            text-decoration: underline;
            color: #2a2a2a;
        }}
        .no-data {{
            text-align: center;
            color: #6a6a6a;
            font-style: italic;
            padding: 40px;
            background-color: #fafafa;
            border: 1px solid #e0e0e0;
        }}
        .timestamp {{
            color: #6a6a6a;
            font-size: 14px;
            text-align: right;
            margin-top: 30px;
            padding-top: 20px;
            border-top: 1px solid #e0e0e0;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Data.gov Vanished Datasets Report</h1>
        
        <div class="stats">
            <h2>Summary Statistics</h2>
            <div class="stat-item">LIL Archive Datasets: {stats.get('lil_datasets', 0)}</div>
            <div class="stat-item">Live Datasets: {stats.get('live_datasets', 0)}</div>
            <div class="stat-item">Vanished Datasets: {stats.get('vanished_datasets', 0)}</div>
            <div class="stat-item">Last Check: {stats.get('last_check', 'Unknown')}</div>
        </div>
        
        <h2>Vanished Datasets</h2>
"""
        
        if vanished_datasets:
            html += """
        <table>
            <thead>
                <tr>
                    <th>Title</th>
                    <th>Agency</th>
                    <th>Original URL</th>
                    <th>Last Seen</th>
                    <th>Suspected Cause</th>
                    <th>Status</th>
                    <th>Archive Link</th>
                </tr>
            </thead>
            <tbody>
"""
            for dataset in vanished_datasets:
                status_class = f"status-{dataset.get('status', 'unknown').lower()}"
                archive_link = dataset.get('archive_link', '#')
                
                html += f"""
                <tr>
                    <td>{dataset.get('title', 'N/A')}</td>
                    <td>{dataset.get('agency', 'N/A')}</td>
                    <td><a href="{dataset.get('original_url', '#')}" target="_blank">{dataset.get('original_url', 'N/A')}</a></td>
                    <td>{dataset.get('last_seen_date', 'N/A')}</td>
                    <td>{dataset.get('suspected_cause', 'N/A')}</td>
                    <td class="{status_class}">{dataset.get('status', 'N/A')}</td>
                    <td><a href="{archive_link}" target="_blank" class="archive-link">View Archive</a></td>
                </tr>
"""
            html += """
            </tbody>
        </table>
"""
        else:
            html += """
        <div class="no-data">
            No vanished datasets found. All datasets from the LIL archive are still present in the live catalog.
        </div>
"""
        
        html += f"""
        <div class="timestamp">
            Report generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        </div>
    </div>
</body>
</html>
"""
        return html
