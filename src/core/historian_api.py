"""
Dataset State Historian API
Comprehensive API endpoints for the historian system
"""

from flask import Flask, jsonify, request, render_template
import sqlite3
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import logging
from pathlib import Path

from src.core.historian_core import DatasetStateHistorian
from src.integrations.lil_integration import LILIntegration
from src.integrations.wayback_enhanced import WaybackEnhanced
from src.integrations.wayback_fetcher import WaybackFetcher
from src.visualization.timeline_ui import TimelineUI

# Post-mortem system will be imported conditionally to avoid WeasyPrint issues
POSTMORTEM_AVAILABLE = False
PostMortemSystem = None

logger = logging.getLogger(__name__)

class HistorianAPI:
    """Comprehensive API for Dataset State Historian"""
    
    def __init__(self, db_path: str = "datasets.db"):
        self.db_path = db_path
        self.historian = DatasetStateHistorian(db_path)
        self.lil = LILIntegration(db_path)
        self.wayback = WaybackEnhanced(db_path)
        self.wayback_fetcher = WaybackFetcher(db_path)
        self.timeline_ui = TimelineUI(db_path)
        
        # Try to import post-mortem system conditionally
        try:
            from src.analysis.postmortem_system import PostMortemSystem
            self.postmortem = PostMortemSystem(db_path)
            POSTMORTEM_AVAILABLE = True
        except ImportError as e:
            logger.warning(f"Post-mortem system not available: {e}")
            self.postmortem = None
            POSTMORTEM_AVAILABLE = False
        
        # Create Flask app
        self.app = Flask(__name__)
        self.app.config['SECRET_KEY'] = 'historian_secret_key'
        
        self._setup_routes()
    
    def _setup_routes(self):
        """Setup all API routes"""
        
        # Core dataset endpoints
        @self.app.route('/api/datasets')
        def get_all_datasets():
            """Get all datasets with basic information"""
            try:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT DISTINCT dataset_id, title, agency, publisher,
                           MIN(snapshot_date) as first_seen,
                           MAX(snapshot_date) as last_seen,
                           COUNT(*) as snapshot_count
                    FROM historian_snapshots
                    GROUP BY dataset_id
                    ORDER BY last_seen DESC
                ''')
                
                datasets = []
                for row in cursor.fetchall():
                    datasets.append({
                        'dataset_id': row[0],
                        'title': row[1],
                        'agency': row[2],
                        'publisher': row[3],
                        'first_seen': row[4],
                        'last_seen': row[5],
                        'snapshot_count': row[6]
                    })
                
                conn.close()
                return jsonify(datasets)
                
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/datasets/<dataset_id>')
        def get_dataset(dataset_id):
            """Get detailed information for a specific dataset"""
            try:
                # Get basic info
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                
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
                    return jsonify({'error': 'Dataset not found'}), 404
                
                # Get volatility score
                cursor.execute('''
                    SELECT AVG(volatility_score) as avg_volatility,
                           COUNT(*) as change_count
                    FROM historian_diffs
                    WHERE dataset_id = ?
                ''', (dataset_id,))
                
                volatility_row = cursor.fetchone()
                avg_volatility = volatility_row[0] if volatility_row[0] else 0.0
                change_count = volatility_row[1] if volatility_row[1] else 0
                
                conn.close()
                
                return jsonify({
                    'dataset_id': dataset_id,
                    'title': row[0],
                    'agency': row[1],
                    'publisher': row[2],
                    'license': row[3],
                    'landing_page': row[4],
                    'modified': row[5],
                    'last_snapshot': row[6],
                    'avg_volatility': round(avg_volatility, 3),
                    'change_count': change_count
                })
                
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/datasets/<dataset_id>/timeline')
        def get_dataset_timeline(dataset_id):
            """Get timeline for a specific dataset"""
            try:
                timeline_data = self.timeline_ui.generate_timeline_summary(dataset_id)
                return jsonify(timeline_data)
                
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/datasets/<dataset_id>/chromogram')
        def get_dataset_chromogram(dataset_id):
            """Get chromogram data for a dataset"""
            try:
                chromogram_data = self.timeline_ui.generate_chromogram_data(dataset_id)
                return jsonify(chromogram_data)
                
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/datasets/<dataset_id>/changes')
        def get_dataset_changes(dataset_id):
            """Get changes for a specific dataset"""
            try:
                changes = self.timeline_ui.generate_change_log(dataset_id)
                return jsonify({'dataset_id': dataset_id, 'changes': changes})
                
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/datasets/<dataset_id>/field/<field_name>')
        def get_field_diff(dataset_id, field_name):
            """Get field diff panel for a specific field"""
            try:
                field_diff = self.timeline_ui.generate_field_diff_panel(dataset_id, field_name)
                return jsonify(field_diff)
                
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/datasets/<dataset_id>/drift')
        def get_content_drift(dataset_id):
            """Get content drift analysis for a dataset"""
            try:
                drift_data = self.timeline_ui.generate_content_drift_panel(dataset_id)
                return jsonify(drift_data)
                
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/datasets/<dataset_id>/overview')
        def get_dataset_overview(dataset_id):
            """Get comprehensive dataset overview"""
            try:
                overview = self.timeline_ui.generate_dataset_overview(dataset_id)
                return jsonify(overview)
                
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        # Volatility and ranking endpoints
        @self.app.route('/api/volatility/ranking')
        def get_volatility_ranking():
            """Get volatility ranking of all datasets"""
            try:
                limit = request.args.get('limit', 50, type=int)
                ranking = self.timeline_ui.generate_volatility_ranking(limit)
                return jsonify({'ranking': ranking})
                
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/volatility/stats')
        def get_volatility_stats():
            """Get overall volatility statistics"""
            try:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                
                # Get volatility distribution
                cursor.execute('''
                    SELECT 
                        CASE 
                            WHEN volatility_score > 0.7 THEN 'high'
                            WHEN volatility_score > 0.4 THEN 'medium'
                            ELSE 'low'
                        END as risk_level,
                        COUNT(*) as count
                    FROM historian_diffs
                    GROUP BY risk_level
                ''')
                
                risk_distribution = {}
                for row in cursor.fetchall():
                    risk_distribution[row[0]] = row[1]
                
                # Get average volatility
                cursor.execute('SELECT AVG(volatility_score) FROM historian_diffs')
                avg_volatility = cursor.fetchone()[0] or 0.0
                
                # Get total datasets
                cursor.execute('SELECT COUNT(DISTINCT dataset_id) FROM historian_snapshots')
                total_datasets = cursor.fetchone()[0] or 0
                
                conn.close()
                
                return jsonify({
                    'total_datasets': total_datasets,
                    'avg_volatility': round(avg_volatility, 3),
                    'risk_distribution': risk_distribution
                })
                
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        # LIL integration endpoints
        @self.app.route('/api/lil/snapshots')
        def get_lil_snapshots():
            """Get available LIL snapshots"""
            try:
                snapshots = self.lil.get_available_snapshots()
                return jsonify({'snapshots': snapshots})
                
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/lil/snapshots/<snapshot_date>')
        def get_lil_snapshot_datasets(snapshot_date):
            """Get datasets for a specific LIL snapshot"""
            try:
                datasets = self.lil.get_snapshot_datasets(snapshot_date)
                return jsonify({'snapshot_date': snapshot_date, 'datasets': datasets})
                
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/lil/discover', methods=['POST'])
        def discover_lil_snapshots():
            """Discover new LIL snapshots"""
            try:
                snapshots = self.lil.discover_available_snapshots()
                return jsonify({'snapshots': snapshots})
                
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/lil/process/<snapshot_date>', methods=['POST'])
        def process_lil_snapshot(snapshot_date):
            """Process a specific LIL snapshot"""
            try:
                # Get snapshot info
                snapshots = self.lil.get_available_snapshots()
                snapshot_info = None
                for snap in snapshots:
                    if snap['snapshot_date'] == snapshot_date:
                        snapshot_info = snap
                        break
                
                if not snapshot_info:
                    return jsonify({'error': 'Snapshot not found'}), 404
                
                # Process snapshot
                success = self.lil.process_snapshot(snapshot_info)
                
                return jsonify({
                    'snapshot_date': snapshot_date,
                    'processed': success
                })
                
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        # Wayback integration endpoints
        @self.app.route('/api/wayback/find/<path:url>')
        def find_wayback_snapshot(url):
            """Find closest Wayback snapshot for a URL"""
            try:
                target_date = request.args.get('date')
                snapshot = self.wayback.find_closest_snapshot(url, target_date)
                
                if snapshot:
                    return jsonify(snapshot)
                else:
                    return jsonify({'error': 'No snapshot found'}), 404
                    
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/wayback/timeline/<path:url>')
        def get_wayback_timeline(url):
            """Get Wayback timeline for a URL"""
            try:
                timeline = self.wayback.get_wayback_timeline(url)
                return jsonify({'url': url, 'timeline': timeline})
                
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/wayback/missing', methods=['POST'])
        def find_missing_datasets():
            """Find missing datasets using Wayback"""
            try:
                data = request.get_json()
                current_datasets = data.get('current_datasets', [])
                historical_datasets = data.get('historical_datasets', [])
                
                missing = self.wayback.find_missing_datasets(current_datasets, historical_datasets)
                return jsonify({'missing': missing})
                
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        # Vanished datasets endpoints
        @self.app.route('/api/vanished/datasets')
        def get_vanished_datasets():
            """Get list of all vanished datasets"""
            try:
                datasets = self.historian.get_vanished_datasets()
                return jsonify({'vanished_datasets': datasets})
                
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/vanished/datasets/<dataset_id>/timeline')
        def get_vanished_dataset_timeline(dataset_id):
            """Get timeline for a vanished dataset"""
            try:
                timeline = self.historian.get_vanished_dataset_timeline(dataset_id)
                return jsonify(timeline)
                
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/vanished/datasets/<dataset_id>/reconstruct', methods=['POST'])
        def reconstruct_vanished_dataset(dataset_id):
            """Reconstruct a vanished dataset from archival sources"""
            try:
                # Get vanished dataset info
                vanished_datasets = self.historian.get_vanished_datasets()
                vanished_info = None
                for vd in vanished_datasets:
                    if vd['dataset_id'] == dataset_id:
                        vanished_info = vd
                        break
                
                if not vanished_info:
                    return jsonify({'error': 'Dataset not found in vanished datasets'}), 404
                
                # Reconstruct dataset
                success = self.wayback_fetcher.reconstruct_vanished_dataset(
                    dataset_id, 
                    {
                        'dataset_id': dataset_id,
                        'last_seen_date': vanished_info['last_seen_date'],
                        'last_seen_source': vanished_info['last_seen_source']
                    }
                )
                
                return jsonify({
                    'dataset_id': dataset_id,
                    'reconstructed': success
                })
                
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/vanished/detect', methods=['POST'])
        def detect_vanished_datasets():
            """Detect newly vanished datasets"""
            try:
                vanished = self.wayback_fetcher.detect_vanished_datasets()
                return jsonify({'vanished_datasets': vanished})
                
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/vanished/reconstruct-all', methods=['POST'])
        def reconstruct_all_vanished():
            """Reconstruct all vanished datasets"""
            try:
                results = self.wayback_fetcher.reconstruct_all_vanished()
                return jsonify(results)
                
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/events/disappearances')
        def get_disappearance_events():
            """Get datasets that have disappeared"""
            try:
                events = self.historian.detect_disappearance_events()
                return jsonify({'disappearance_events': events})
                
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        # Post-mortem endpoints
        @self.app.route('/api/postmortem/<dataset_id>')
        def get_postmortem(dataset_id):
            """Generate post-mortem analysis for a dataset"""
            if not self.postmortem:
                return jsonify({'error': 'Post-mortem system not available (missing dependencies)'}), 503
                
            try:
                postmortem_data = self.postmortem.generate_postmortem(dataset_id)
                return jsonify(postmortem_data)
                
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/postmortem/<dataset_id>/html')
        def get_postmortem_html(dataset_id):
            """Generate HTML post-mortem report"""
            if not self.postmortem:
                return jsonify({'error': 'Post-mortem system not available (missing dependencies)'}), 503
                
            try:
                postmortem_data = self.postmortem.generate_postmortem(dataset_id)
                if 'error' in postmortem_data:
                    return jsonify(postmortem_data), 404
                
                html_file = self.postmortem.generate_html_report(postmortem_data)
                if html_file:
                    return jsonify({'html_file': html_file})
                else:
                    return jsonify({'error': 'Failed to generate HTML report'}), 500
                    
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/postmortem/<dataset_id>/pdf')
        def get_postmortem_pdf(dataset_id):
            """Generate PDF post-mortem report"""
            if not self.postmortem:
                return jsonify({'error': 'Post-mortem system not available (missing dependencies)'}), 503
                
            try:
                postmortem_data = self.postmortem.generate_postmortem(dataset_id)
                if 'error' in postmortem_data:
                    return jsonify(postmortem_data), 404
                
                pdf_file = self.postmortem.generate_pdf_report(postmortem_data)
                if pdf_file:
                    return jsonify({'pdf_file': pdf_file})
                else:
                    return jsonify({'error': 'Failed to generate PDF report'}), 500
                    
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/postmortem/batch', methods=['POST'])
        def generate_batch_postmortems():
            """Generate post-mortem reports for multiple datasets"""
            if not self.postmortem:
                return jsonify({'error': 'Post-mortem system not available (missing dependencies)'}), 503
                
            try:
                data = request.get_json()
                dataset_ids = data.get('dataset_ids', [])
                
                if not dataset_ids:
                    return jsonify({'error': 'No dataset IDs provided'}), 400
                
                results = self.postmortem.generate_batch_postmortems(dataset_ids)
                return jsonify(results)
                
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        # System status endpoints
        @self.app.route('/api/status')
        def get_system_status():
            """Get overall system status"""
            try:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                
                # Get basic counts
                cursor.execute('SELECT COUNT(DISTINCT dataset_id) FROM historian_snapshots')
                total_datasets = cursor.fetchone()[0] or 0
                
                cursor.execute('SELECT COUNT(*) FROM historian_snapshots')
                total_snapshots = cursor.fetchone()[0] or 0
                
                cursor.execute('SELECT COUNT(*) FROM historian_diffs')
                total_diffs = cursor.fetchone()[0] or 0
                
                cursor.execute('SELECT COUNT(*) FROM lil_snapshots')
                lil_snapshots = cursor.fetchone()[0] or 0
                
                cursor.execute('SELECT COUNT(*) FROM wayback_snapshots')
                wayback_snapshots = cursor.fetchone()[0] or 0
                
                conn.close()
                
                return jsonify({
                    'total_datasets': total_datasets,
                    'total_snapshots': total_snapshots,
                    'total_diffs': total_diffs,
                    'lil_snapshots': lil_snapshots,
                    'wayback_snapshots': wayback_snapshots,
                    'last_updated': datetime.now().isoformat()
                })
                
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        # Main dashboard route
        @self.app.route('/')
        def dashboard():
            """Main dashboard page"""
            return render_template('components/historian_dashboard.html')
        
        # Timeline explorer route
        @self.app.route('/timeline')
        def timeline_explorer():
            """Timeline explorer page"""
            return render_template('pages/timeline_explorer.html')
        
        # Post-mortem viewer route
        @self.app.route('/postmortem/<dataset_id>')
        def postmortem_viewer(dataset_id):
            """Post-mortem viewer page"""
            return render_template('pages/postmortem_report.html', dataset_id=dataset_id)
    
    def run(self, host='127.0.0.1', port=8082, debug=True):
        """Run the API server"""
        self.app.run(host=host, port=port, debug=debug)

if __name__ == '__main__':
    api = HistorianAPI()
    api.run()
