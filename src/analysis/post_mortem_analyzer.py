"""
Post-mortem Analyzer: Generate forensic analysis of vanished datasets
"""

import json
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from pathlib import Path
import logging
import requests
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

class PostMortemAnalyzer:
    def __init__(self, db_path: str = "datasets.db", base_dir: str = "dataset_states"):
        self.db_path = db_path
        self.base_dir = Path(base_dir)
        self.postmortems_dir = Path("postmortems")
        self.postmortems_dir.mkdir(exist_ok=True)
    
    def analyze_vanished_dataset(self, dataset_id: str) -> Dict:
        """Generate comprehensive post-mortem analysis for a vanished dataset"""
        logger.info(f"Analyzing vanished dataset: {dataset_id}")
        
        # Get dataset history
        history = self._get_dataset_history(dataset_id)
        if not history:
            return {'error': 'No history found for dataset'}
        
        # Get final state before disappearance
        final_state = self._get_final_state(dataset_id)
        
        # Analyze disappearance patterns
        disappearance_analysis = self._analyze_disappearance_pattern(history, final_state)
        
        # Check current URL status
        url_analysis = self._analyze_url_status(final_state)
        
        # Generate forensic timeline
        forensic_timeline = self._generate_forensic_timeline(history)
        
        # Determine suspected cause
        suspected_cause = self._determine_suspected_cause(disappearance_analysis, url_analysis, forensic_timeline)
        
        # Generate post-mortem report
        postmortem = {
            'dataset_id': dataset_id,
            'analysis_date': datetime.now().isoformat(),
            'final_state': final_state,
            'disappearance_analysis': disappearance_analysis,
            'url_analysis': url_analysis,
            'forensic_timeline': forensic_timeline,
            'suspected_cause': suspected_cause,
            'confidence_score': self._calculate_confidence_score(disappearance_analysis, url_analysis),
            'recommendations': self._generate_recommendations(suspected_cause, url_analysis)
        }
        
        # Save post-mortem
        self._save_postmortem(dataset_id, postmortem)
        
        return postmortem
    
    def _get_dataset_history(self, dataset_id: str) -> List[Dict]:
        """Get complete history of dataset states"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT snapshot_date, row_count, column_count, file_size, created_at
            FROM dataset_states 
            WHERE dataset_id = ?
            ORDER BY snapshot_date ASC
        ''', (dataset_id,))
        
        columns = [description[0] for description in cursor.description]
        history = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        conn.close()
        return history
    
    def _get_final_state(self, dataset_id: str) -> Optional[Dict]:
        """Get the final known state before disappearance"""
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
    
    def _analyze_disappearance_pattern(self, history: List[Dict], final_state: Optional[Dict]) -> Dict:
        """Analyze patterns leading to disappearance"""
        if not history:
            return {'pattern': 'no_history', 'confidence': 0}
        
        # Analyze timeline patterns
        snapshots = len(history)
        first_seen = datetime.strptime(history[0]['snapshot_date'], '%Y-%m-%d')
        last_seen = datetime.strptime(history[-1]['snapshot_date'], '%Y-%m-%d')
        age_days = (last_seen - first_seen).days
        
        # Check for gradual decline
        row_counts = [h['row_count'] for h in history if h['row_count']]
        declining_trend = False
        if len(row_counts) > 2:
            recent_avg = sum(row_counts[-3:]) / len(row_counts[-3:])
            earlier_avg = sum(row_counts[:3]) / len(row_counts[:3])
            declining_trend = recent_avg < earlier_avg * 0.8  # 20% decline
        
        # Check for stability before disappearance
        stable_before_disappearance = False
        if len(row_counts) >= 3:
            last_three = row_counts[-3:]
            stable_before_disappearance = max(last_three) - min(last_three) < max(last_three) * 0.1  # <10% variation
        
        return {
            'pattern': 'gradual_decline' if declining_trend else 'sudden_disappearance',
            'snapshots': snapshots,
            'age_days': age_days,
            'declining_trend': declining_trend,
            'stable_before_disappearance': stable_before_disappearance,
            'final_row_count': row_counts[-1] if row_counts else None,
            'confidence': 0.8 if declining_trend else 0.6
        }
    
    def _analyze_url_status(self, final_state: Optional[Dict]) -> Dict:
        """Analyze current URL status and potential causes"""
        if not final_state or not final_state.get('url'):
            return {'status': 'no_url', 'confidence': 0}
        
        url = final_state['url']
        
        try:
            # Check current URL status
            response = requests.head(url, timeout=10, allow_redirects=True)
            status_code = response.status_code
            
            # Analyze response
            if status_code == 200:
                return {'status': 'accessible', 'status_code': status_code, 'confidence': 0.9}
            elif status_code == 404:
                return {'status': 'not_found', 'status_code': status_code, 'confidence': 0.8}
            elif status_code == 403:
                return {'status': 'forbidden', 'status_code': status_code, 'confidence': 0.7}
            elif status_code == 301 or status_code == 302:
                redirect_url = response.headers.get('Location', '')
                return {
                    'status': 'redirected',
                    'status_code': status_code,
                    'redirect_url': redirect_url,
                    'confidence': 0.6
                }
            else:
                return {'status': 'error', 'status_code': status_code, 'confidence': 0.5}
                
        except requests.exceptions.Timeout:
            return {'status': 'timeout', 'confidence': 0.4}
        except requests.exceptions.ConnectionError:
            return {'status': 'connection_error', 'confidence': 0.3}
        except Exception as e:
            return {'status': 'error', 'error': str(e), 'confidence': 0.2}
    
    def _generate_forensic_timeline(self, history: List[Dict]) -> List[Dict]:
        """Generate forensic timeline of dataset changes"""
        timeline = []
        
        for i, state in enumerate(history):
            event = {
                'date': state['snapshot_date'],
                'event_type': 'snapshot',
                'row_count': state['row_count'],
                'column_count': state['column_count'],
                'file_size': state['file_size']
            }
            
            # Add context based on position in timeline
            if i == 0:
                event['context'] = 'first_snapshot'
            elif i == len(history) - 1:
                event['context'] = 'final_snapshot'
            else:
                # Compare with previous state
                prev_state = history[i-1]
                if state['row_count'] and prev_state['row_count']:
                    row_delta = state['row_count'] - prev_state['row_count']
                    if abs(row_delta) > prev_state['row_count'] * 0.1:  # >10% change
                        event['context'] = f'significant_change_{row_delta:+d}_rows'
                    else:
                        event['context'] = 'stable'
                else:
                    event['context'] = 'unknown'
            
            timeline.append(event)
        
        return timeline
    
    def _determine_suspected_cause(self, disappearance_analysis: Dict, url_analysis: Dict, 
                                 forensic_timeline: List[Dict]) -> Dict:
        """Determine the most likely cause of disappearance"""
        
        # URL-based causes
        if url_analysis['status'] == 'not_found':
            return {
                'cause': 'url_removed',
                'description': 'Dataset URL returns 404 - likely removed from source',
                'confidence': 0.8,
                'evidence': ['url_404', 'no_redirect']
            }
        elif url_analysis['status'] == 'redirected':
            return {
                'cause': 'url_moved',
                'description': 'Dataset URL redirects to new location - may have been moved',
                'confidence': 0.7,
                'evidence': ['url_redirect', 'new_location']
            }
        elif url_analysis['status'] == 'forbidden':
            return {
                'cause': 'access_restricted',
                'description': 'Dataset access restricted - may require authentication',
                'confidence': 0.6,
                'evidence': ['url_403', 'access_denied']
            }
        
        # Pattern-based causes
        if disappearance_analysis['pattern'] == 'gradual_decline':
            return {
                'cause': 'gradual_degradation',
                'description': 'Dataset gradually declined before disappearance - possible data quality issues',
                'confidence': 0.7,
                'evidence': ['declining_trend', 'gradual_changes']
            }
        elif disappearance_analysis['stable_before_disappearance']:
            return {
                'cause': 'sudden_removal',
                'description': 'Dataset was stable then suddenly disappeared - likely intentional removal',
                'confidence': 0.8,
                'evidence': ['stable_then_gone', 'sudden_change']
            }
        
        # Default cause
        return {
            'cause': 'unknown',
            'description': 'Unable to determine cause - requires manual investigation',
            'confidence': 0.3,
            'evidence': ['insufficient_data']
        }
    
    def _calculate_confidence_score(self, disappearance_analysis: Dict, url_analysis: Dict) -> float:
        """Calculate overall confidence score for the analysis"""
        disappearance_conf = disappearance_analysis.get('confidence', 0)
        url_conf = url_analysis.get('confidence', 0)
        
        # Weighted average
        return (disappearance_conf * 0.6 + url_conf * 0.4)
    
    def _generate_recommendations(self, suspected_cause: Dict, url_analysis: Dict) -> List[str]:
        """Generate recommendations based on analysis"""
        recommendations = []
        
        cause = suspected_cause.get('cause', 'unknown')
        
        if cause == 'url_removed':
            recommendations.extend([
                "Check if dataset was moved to a new URL",
                "Contact the publishing agency for clarification",
                "Search for alternative sources or mirrors",
                "Check if dataset is available in draft state"
            ])
        elif cause == 'url_moved':
            recommendations.extend([
                "Follow the redirect URL to find new location",
                "Update dataset references to new URL",
                "Verify data integrity at new location"
            ])
        elif cause == 'access_restricted':
            recommendations.extend([
                "Check if authentication is required",
                "Contact agency about access requirements",
                "Look for public alternatives"
            ])
        elif cause == 'gradual_degradation':
            recommendations.extend([
                "Investigate data quality issues",
                "Check if dataset was deprecated",
                "Look for replacement datasets"
            ])
        elif cause == 'sudden_removal':
            recommendations.extend([
                "Contact agency about removal reason",
                "Check for policy changes",
                "Look for archived versions"
            ])
        else:
            recommendations.extend([
                "Manual investigation required",
                "Check multiple data sources",
                "Contact relevant agencies"
            ])
        
        return recommendations
    
    def _save_postmortem(self, dataset_id: str, postmortem: Dict):
        """Save post-mortem analysis to file"""
        postmortem_file = self.postmortems_dir / f"{dataset_id}_postmortem.json"
        
        with open(postmortem_file, 'w') as f:
            json.dump(postmortem, f, indent=2, default=str)
        
        logger.info(f"Post-mortem saved: {postmortem_file}")
    
    def generate_postmortem_report(self, dataset_id: str) -> str:
        """Generate HTML post-mortem report"""
        postmortem = self.analyze_vanished_dataset(dataset_id)
        
        if 'error' in postmortem:
            return f"Error: {postmortem['error']}"
        
        # Generate HTML report
        html = self._generate_postmortem_html(postmortem)
        
        # Save report
        report_file = self.postmortems_dir / f"{dataset_id}_postmortem.html"
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(html)
        
        logger.info(f"Post-mortem report saved: {report_file}")
        return str(report_file)
    
    def _generate_postmortem_html(self, postmortem: Dict) -> str:
        """Generate HTML post-mortem report"""
        
        html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Post-mortem Analysis - {postmortem['dataset_id']}</title>
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
            color: #d32f2f;
            border-bottom: 2px solid #d32f2f;
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
        .alert {{
            background-color: #ffebee;
            border: 1px solid #f44336;
            padding: 15px;
            margin: 20px 0;
            border-radius: 0;
        }}
        .confidence-high {{ color: #4caf50; }}
        .confidence-medium {{ color: #f57c00; }}
        .confidence-low {{ color: #f44336; }}
        .evidence-list {{
            background-color: #f5f5f5;
            padding: 15px;
            margin: 10px 0;
        }}
        .recommendations {{
            background-color: #e8f5e8;
            padding: 15px;
            margin: 20px 0;
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
    </style>
</head>
<body>
    <div class="container">
        <h1>🔍 Post-mortem Analysis: {postmortem['dataset_id']}</h1>
        
        <div class="alert">
            <strong>Dataset Status:</strong> VANISHED<br>
            <strong>Analysis Date:</strong> {postmortem['analysis_date']}<br>
            <strong>Confidence Score:</strong> <span class="confidence-{'high' if postmortem['confidence_score'] > 0.7 else 'medium' if postmortem['confidence_score'] > 0.4 else 'low'}">{postmortem['confidence_score']:.1%}</span>
        </div>
        
        <h2>🎯 Suspected Cause</h2>
        <div class="evidence-list">
            <strong>{postmortem['suspected_cause']['description']}</strong><br>
            <strong>Confidence:</strong> {postmortem['suspected_cause']['confidence']:.1%}<br>
            <strong>Evidence:</strong> {', '.join(postmortem['suspected_cause']['evidence'])}
        </div>
        
        <h2>📊 Disappearance Analysis</h2>
        <table>
            <tr><td>Pattern</td><td>{postmortem['disappearance_analysis']['pattern']}</td></tr>
            <tr><td>Snapshots</td><td>{postmortem['disappearance_analysis']['snapshots']}</td></tr>
            <tr><td>Age (days)</td><td>{postmortem['disappearance_analysis']['age_days']}</td></tr>
            <tr><td>Declining Trend</td><td>{'Yes' if postmortem['disappearance_analysis']['declining_trend'] else 'No'}</td></tr>
            <tr><td>Stable Before Disappearance</td><td>{'Yes' if postmortem['disappearance_analysis']['stable_before_disappearance'] else 'No'}</td></tr>
        </table>
        
        <h2>🌐 URL Analysis</h2>
        <table>
            <tr><td>Status</td><td>{postmortem['url_analysis']['status']}</td></tr>
            <tr><td>Status Code</td><td>{postmortem['url_analysis'].get('status_code', 'N/A')}</td></tr>
            <tr><td>Confidence</td><td>{postmortem['url_analysis']['confidence']:.1%}</td></tr>
        </table>
        
        <h2>⏰ Forensic Timeline</h2>
        <table>
            <thead>
                <tr>
                    <th>Date</th>
                    <th>Event</th>
                    <th>Context</th>
                    <th>Rows</th>
                    <th>Columns</th>
                </tr>
            </thead>
            <tbody>
"""
        
        for event in postmortem['forensic_timeline']:
            html += f"""
                <tr>
                    <td>{event['date']}</td>
                    <td>{event['event_type']}</td>
                    <td>{event['context']}</td>
                    <td>{event['row_count'] or 'N/A'}</td>
                    <td>{event['column_count'] or 'N/A'}</td>
                </tr>
"""
        
        html += """
            </tbody>
        </table>
        
        <h2>💡 Recommendations</h2>
        <div class="recommendations">
            <ul>
"""
        
        for rec in postmortem['recommendations']:
            html += f"<li>{rec}</li>"
        
        html += f"""
            </ul>
        </div>
        
        <div style="margin-top: 40px; padding-top: 20px; border-top: 1px solid #e0e0e0; color: #6a6a6a; font-size: 14px;">
            Post-mortem generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        </div>
    </div>
</body>
</html>
"""
        
        return html
