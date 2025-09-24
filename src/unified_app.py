"""
Unified Concordance App: Single Application with Subpages
Combines all monitoring, data viewing, and real-time features
"""

from flask import Flask, render_template, jsonify, request, redirect, url_for
from flask_socketio import SocketIO, emit
import sqlite3
import json
import asyncio
import threading
from datetime import datetime, timedelta
from pathlib import Path
import os
from monitoring.enhanced_monitor import EnhancedConcordanceMonitor
from analysis.time_series_manager import TimeSeriesManager
from visualization.timeline_visualizer import TimelineVisualizer
from analysis.agency_analytics import AgencyAnalytics
from integrations.lil_integration import LILIntegration
from analysis.enhanced_diff_engine import EnhancedDiffEngine
from visualization.chromogram_timeline import ChromogramTimeline
from processing.full_database_processor import FullDatabaseProcessor
from monitoring.scaled_monitor import ScaledMonitor
from processing.enhanced_row_column_computer import EnhancedRowColumnComputer
from processing.backfill_dimensions import DimensionBackfillProcessor

# New enhanced systems
from core.availability_detector import AvailabilityDetector, DatasetStatus
from analysis.enhanced_diff_engine_v2 import EnhancedDiffEngineV2
from analysis.event_extractor import EventExtractor, EventType, EventSeverity
from visualization.chromogram_timeline_v2 import ChromogramTimelineV2
from api.enhanced_endpoints import enhanced_bp
from api.notifications_api import notifications_bp

app = Flask(__name__, 
           template_folder='../web/templates',
           static_folder='../web/static')
app.config['SECRET_KEY'] = 'concordance_secret_key'
socketio = SocketIO(app, cors_allowed_origins="*")

# Register blueprints
app.register_blueprint(enhanced_bp)

# Import and register additional API blueprints
from api.datasets_api import datasets_bp
from api.wayback_api import wayback_bp
from api.analytics_api import analytics_bp
app.register_blueprint(datasets_bp)
app.register_blueprint(wayback_bp)
app.register_blueprint(analytics_bp)
app.register_blueprint(notifications_bp)

# Global instances
monitor = EnhancedConcordanceMonitor()
time_series_manager = TimeSeriesManager()
timeline_visualizer = TimelineVisualizer()
agency_analytics = AgencyAnalytics()
lil_integration = LILIntegration()
enhanced_diff_engine = EnhancedDiffEngine()
chromogram_timeline = ChromogramTimeline()
full_database_processor = FullDatabaseProcessor()
scaled_monitor = ScaledMonitor()
dimension_computer = EnhancedRowColumnComputer()
dimension_backfill = DimensionBackfillProcessor()
monitoring_thread = None
full_processing_thread = None

def get_database_connection():
    """Get database connection"""
    return sqlite3.connect(monitor.db_path)

# ============================================================================
# WEBSOCKET EVENTS
# ============================================================================

@socketio.on('connect')
def handle_connect():
    """Handle client connection"""
    print('Client connected')
    emit('status', {'message': 'Connected to Concordance monitoring system'})

@socketio.on('disconnect')
def handle_disconnect():
    """Handle client disconnection"""
    print('Client disconnected')

@socketio.on('request_update')
def handle_update_request():
    """Handle real-time update request"""
    try:
        # Get current stats
        conn = get_database_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT COUNT(*) FROM dataset_states')
        total_snapshots = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(DISTINCT dataset_id) FROM dataset_states')
        unique_datasets = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM state_diffs')
        total_diffs = cursor.fetchone()[0]
        
        # Get availability stats from metadata files
        availability_stats = {'available': 0, 'unavailable': 0, 'error': 0, 'unknown': 0}
        
        dataset_states_dir = Path("dataset_states")
        if dataset_states_dir.exists():
            for dataset_dir in dataset_states_dir.iterdir():
                if dataset_dir.is_dir():
                    snapshot_dirs = [d for d in dataset_dir.iterdir() if d.is_dir()]
                    if snapshot_dirs:
                        latest_snapshot = max(snapshot_dirs, key=lambda x: x.name)
                        metadata_file = latest_snapshot / 'metadata.json'
                        
                        if metadata_file.exists():
                            try:
                                with open(metadata_file, 'r') as f:
                                    metadata = json.load(f)
                                    availability = metadata.get('availability', 'unknown')
                                    availability_stats[availability] = availability_stats.get(availability, 0) + 1
                            except:
                                availability_stats['unknown'] += 1
        
        conn.close()
        
        # Emit real-time update with proper timestamp
        current_time = datetime.now()
        emit('stats_update', {
            'total_snapshots': total_snapshots,
            'unique_datasets': unique_datasets,
            'total_diffs': total_diffs,
            'availability_stats': availability_stats,
            'monitoring_status': 'running' if monitoring_thread and monitoring_thread.is_alive() else 'stopped',
            'last_updated': current_time.isoformat(),
            'timestamp': current_time.strftime('%Y-%m-%d %H:%M:%S')
        })
        
    except Exception as e:
        emit('error', {'message': f'Error getting update: {str(e)}'})

# ============================================================================
# ROUTES
# ============================================================================

@app.route('/')
def index():
    """Main dashboard page"""
    return render_template('pages/unified_app.html', page='dashboard')

@app.route('/favicon.ico')
def favicon():
    """Serve favicon to prevent 404 errors"""
    return '', 204  # No content response

@app.route('/full-database')
def full_database_dashboard():
    """Full database processing dashboard"""
    return render_template('pages/full_database_dashboard.html')

@app.route('/minimal')
def minimal_dashboard():
    """Minimalist dashboard with Cursor Activity Logger design"""
    return render_template('pages/unified_app.html')

@app.route('/dashboard')
def dashboard():
    """Dashboard subpage"""
    return render_template('pages/unified_app.html', page='dashboard')

@app.route('/data-viewer')
def data_viewer():
    """Data viewer subpage"""
    return render_template('pages/unified_app.html', page='data-viewer')

@app.route('/reports')
def reports():
    """Reports subpage"""
    return render_template('pages/unified_app.html', page='reports')

@app.route('/timeline')
def timeline():
    """Timeline dashboard page"""
    return render_template('pages/timeline_dashboard.html')

@app.route('/vanished')
def vanished_datasets():
    """Vanished datasets page"""
    return render_template('pages/vanished_datasets.html')

@app.route('/management')
def management():
    """Management dashboard page"""
    return render_template('pages/management_dashboard.html')

# Enhanced pages
@app.route('/metrics')
def metrics_dashboard():
    """System metrics dashboard"""
    return render_template('pages/analytics.html')

@app.route('/catalog')
def catalog_explorer():
    """Catalog explorer page"""
    return render_template('pages/catalog_explorer.html')

@app.route('/tags')
def tag_gallery():
    """Tag gallery page"""
    return render_template('pages/tag_gallery.html')

@app.route('/api/tags')
def api_tags():
    """API endpoint for tag gallery data"""
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Get agency-based tags (grouped by agency)
        cursor.execute('''
            SELECT 
                ds.agency as name,
                'agency' as category,
                COUNT(DISTINCT ds.dataset_id) as dataset_count,
                COUNT(DISTINCT CASE WHEN ds.availability = 'available' THEN ds.dataset_id END) as available_count,
                COUNT(DISTINCT CASE WHEN ds.availability = 'unavailable' THEN ds.dataset_id END) as unavailable_count,
                AVG(CASE 
                    WHEN ds.row_count IS NOT NULL AND ds.column_count IS NOT NULL 
                    THEN (ds.row_count * ds.column_count) / 1000000.0 
                    ELSE 0.5 
                END) as volatility
            FROM dataset_states ds
            INNER JOIN (
                SELECT dataset_id, MAX(created_at) as max_created
                FROM dataset_states 
                GROUP BY dataset_id
            ) latest ON ds.dataset_id = latest.dataset_id 
            AND ds.created_at = latest.max_created
            WHERE ds.agency IS NOT NULL AND ds.agency != ''
            GROUP BY ds.agency
            HAVING dataset_count >= 10
            ORDER BY dataset_count DESC
        ''')
        
        agency_tags = []
        for row in cursor.fetchall():
            agency_tags.append({
                'name': row[0],
                'category': row[1],
                'dataset_count': row[2],
                'available_count': row[3],
                'unavailable_count': row[4],
                'volatility': row[5] or 0.5,
                'description': f'Datasets from {row[0]}'
            })
        
        # Get format-based tags (grouped by resource_format)
        cursor.execute('''
            SELECT 
                ds.resource_format as name,
                'format' as category,
                COUNT(DISTINCT ds.dataset_id) as dataset_count,
                COUNT(DISTINCT CASE WHEN ds.availability = 'available' THEN ds.dataset_id END) as available_count,
                COUNT(DISTINCT CASE WHEN ds.availability = 'unavailable' THEN ds.dataset_id END) as unavailable_count,
                AVG(CASE 
                    WHEN ds.row_count IS NOT NULL AND ds.column_count IS NOT NULL 
                    THEN (ds.row_count * ds.column_count) / 1000000.0 
                    ELSE 0.5 
                END) as volatility
            FROM dataset_states ds
            INNER JOIN (
                SELECT dataset_id, MAX(created_at) as max_created
                FROM dataset_states 
                GROUP BY dataset_id
            ) latest ON ds.dataset_id = latest.dataset_id 
            AND ds.created_at = latest.max_created
            WHERE ds.resource_format IS NOT NULL AND ds.resource_format != ''
            GROUP BY ds.resource_format
            HAVING dataset_count >= 5
            ORDER BY dataset_count DESC
        ''')
        
        format_tags = []
        for row in cursor.fetchall():
            format_tags.append({
                'name': row[0],
                'category': row[1],
                'dataset_count': row[2],
                'available_count': row[3],
                'unavailable_count': row[4],
                'volatility': row[5] or 0.5,
                'description': f'Datasets in {row[0]} format'
            })
        
        # Get topic-based tags (extracted from titles)
        cursor.execute('''
            SELECT 
                CASE 
                    WHEN ds.title LIKE '%health%' OR ds.title LIKE '%medical%' OR ds.title LIKE '%healthcare%' THEN 'Health & Medical'
                    WHEN ds.title LIKE '%education%' OR ds.title LIKE '%school%' OR ds.title LIKE '%student%' THEN 'Education'
                    WHEN ds.title LIKE '%transportation%' OR ds.title LIKE '%traffic%' OR ds.title LIKE '%road%' THEN 'Transportation'
                    WHEN ds.title LIKE '%environment%' OR ds.title LIKE '%climate%' OR ds.title LIKE '%weather%' THEN 'Environment'
                    WHEN ds.title LIKE '%crime%' OR ds.title LIKE '%police%' OR ds.title LIKE '%safety%' THEN 'Public Safety'
                    WHEN ds.title LIKE '%economic%' OR ds.title LIKE '%business%' OR ds.title LIKE '%financial%' THEN 'Economy'
                    WHEN ds.title LIKE '%housing%' OR ds.title LIKE '%real estate%' OR ds.title LIKE '%property%' THEN 'Housing'
                    WHEN ds.title LIKE '%demographic%' OR ds.title LIKE '%population%' OR ds.title LIKE '%census%' THEN 'Demographics'
                    ELSE NULL
                END as topic,
                COUNT(DISTINCT ds.dataset_id) as dataset_count,
                COUNT(DISTINCT CASE WHEN ds.availability = 'available' THEN ds.dataset_id END) as available_count,
                COUNT(DISTINCT CASE WHEN ds.availability = 'unavailable' THEN ds.dataset_id END) as unavailable_count,
                AVG(CASE 
                    WHEN ds.row_count IS NOT NULL AND ds.column_count IS NOT NULL 
                    THEN (ds.row_count * ds.column_count) / 1000000.0 
                    ELSE 0.5 
                END) as volatility
            FROM dataset_states ds
            INNER JOIN (
                SELECT dataset_id, MAX(created_at) as max_created
                FROM dataset_states 
                GROUP BY dataset_id
            ) latest ON ds.dataset_id = latest.dataset_id 
            AND ds.created_at = latest.max_created
            WHERE ds.title IS NOT NULL AND ds.title != ''
            GROUP BY topic
            HAVING topic IS NOT NULL AND dataset_count >= 5
            ORDER BY dataset_count DESC
        ''')
        
        topic_tags = []
        for row in cursor.fetchall():
            topic_tags.append({
                'name': row[0],
                'category': 'topic',
                'dataset_count': row[1],
                'available_count': row[2],
                'unavailable_count': row[3],
                'volatility': row[4] or 0.5,
                'description': f'Datasets related to {row[0].lower()}'
            })
        
        conn.close()
        
        # Combine all tags
        all_tags = agency_tags + format_tags + topic_tags
        
        return jsonify({
            'tags': all_tags,
            'total_count': len(all_tags),
            'categories': {
                'agency': len(agency_tags),
                'format': len(format_tags),
                'topic': len(topic_tags)
            }
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/licenses')
def api_licenses():
    """Get license distribution and statistics"""
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Get license distribution
        cursor.execute('''
            SELECT 
                COALESCE(hs.license, 'Unknown') as license,
                COUNT(*) as dataset_count,
                COUNT(CASE WHEN ds.availability = 'available' THEN 1 END) as available_count,
                COUNT(CASE WHEN ds.availability = 'unavailable' THEN 1 END) as unavailable_count
            FROM dataset_states ds
            INNER JOIN (
                SELECT dataset_id, MAX(created_at) as max_created
                FROM dataset_states 
                GROUP BY dataset_id
            ) latest ON ds.dataset_id = latest.dataset_id 
            AND ds.created_at = latest.max_created
            LEFT JOIN (
                SELECT dataset_id, license,
                       ROW_NUMBER() OVER (PARTITION BY dataset_id ORDER BY created_at DESC) as rn
                FROM historian_snapshots
            ) hs ON ds.dataset_id = hs.dataset_id AND hs.rn = 1
            GROUP BY COALESCE(hs.license, 'Unknown')
            ORDER BY dataset_count DESC
        ''')
        
        license_data = cursor.fetchall()
        
        # Calculate total datasets
        total_datasets = sum(row[1] for row in license_data)
        
        # Process license data with intelligent classification
        from core.license_classifier import license_classifier
        
        licenses = []
        license_categories = {}
        
        for row in license_data:
            raw_license, dataset_count, available_count, unavailable_count = row
            
            # Classify the license
            if raw_license and raw_license != 'Unknown':
                # Try to parse if it's JSON
                try:
                    if raw_license.startswith('{'):
                        license_dict = json.loads(raw_license)
                        license_text = license_dict.get('name', '') or license_dict.get('raw_text', '')
                        license_url = license_dict.get('url', '') or license_dict.get('raw_url', '')
                    else:
                        license_text = raw_license
                        license_url = None
                except:
                    license_text = raw_license
                    license_url = None
                
                classified = license_classifier.classify_license(license_text, license_url)
            else:
                classified = license_classifier.known_licenses['unknown']
            
            # Group by category
            category = classified.category
            if category not in license_categories:
                license_categories[category] = {
                    'category': category,
                    'name': classified.name,
                    'description': classified.description,
                    'is_open': classified.is_open,
                    'requires_attribution': classified.requires_attribution,
                    'allows_commercial': classified.allows_commercial,
                    'allows_derivatives': classified.allows_derivatives,
                    'share_alike': classified.share_alike,
                    'dataset_count': 0,
                    'available_count': 0,
                    'unavailable_count': 0,
                    'raw_licenses': []
                }
            
            license_categories[category]['dataset_count'] += dataset_count
            license_categories[category]['available_count'] += available_count or 0
            license_categories[category]['unavailable_count'] += unavailable_count or 0
            license_categories[category]['raw_licenses'].append({
                'raw_text': raw_license,
                'count': dataset_count
            })
        
        # Convert to list and calculate percentages
        for category_data in license_categories.values():
            category_data['percentage'] = round((category_data['dataset_count'] / total_datasets) * 100, 1) if total_datasets > 0 else 0
            category_data['availability_rate'] = round((category_data['available_count'] / category_data['dataset_count']) * 100, 1) if category_data['dataset_count'] > 0 else 0
            licenses.append(category_data)
        
        # Sort by dataset count
        licenses.sort(key=lambda x: x['dataset_count'], reverse=True)
        
        # Calculate summary statistics
        unknown_count = next((l['dataset_count'] for l in licenses if l['category'] == 'unknown'), 0)
        known_count = total_datasets - unknown_count
        open_count = sum(l['dataset_count'] for l in licenses if l['is_open'])
        proprietary_count = sum(l['dataset_count'] for l in licenses if not l['is_open'] and l['category'] != 'unknown')
        known_percentage = round(known_count / total_datasets * 100, 1) if total_datasets > 0 else 0
        
        conn.close()
        
        return jsonify({
            'licenses': licenses,
            'summary': {
                'total_datasets': total_datasets,
                'unknown_license_count': unknown_count,
                'known_license_count': known_count,
                'open_license_count': open_count,
                'proprietary_license_count': proprietary_count,
                'unknown_license_percentage': round(unknown_count / total_datasets * 100, 1) if total_datasets > 0 else 0,
                'known_license_percentage': known_percentage,
                'open_license_percentage': round(open_count / total_datasets * 100, 1) if total_datasets > 0 else 0
            }
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/tags/<tag_name>')
def api_tag_details(tag_name):
    """API endpoint for specific tag details"""
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Decode URL-encoded tag name
        tag_name = tag_name.replace('%20', ' ').replace('%2C', ',')
        
        # Determine if it's an agency, format, or topic tag
        cursor.execute('''
            SELECT 
                ds.agency,
                ds.resource_format,
                ds.title,
                COUNT(DISTINCT ds.dataset_id) as dataset_count,
                COUNT(DISTINCT CASE WHEN ds.availability = 'available' THEN ds.dataset_id END) as available_count,
                COUNT(DISTINCT CASE WHEN ds.availability = 'unavailable' THEN ds.dataset_id END) as unavailable_count,
                AVG(CASE 
                    WHEN ds.row_count IS NOT NULL AND ds.column_count IS NOT NULL 
                    THEN (ds.row_count * ds.column_count) / 1000000.0 
                    ELSE 0.5 
                END) as volatility
            FROM dataset_states ds
            INNER JOIN (
                SELECT dataset_id, MAX(created_at) as max_created
                FROM dataset_states 
                GROUP BY dataset_id
            ) latest ON ds.dataset_id = latest.dataset_id 
            AND ds.created_at = latest.max_created
            WHERE (ds.agency = ? OR ds.resource_format = ? OR 
                   (ds.title LIKE ? AND ds.title IS NOT NULL))
            GROUP BY ds.agency, ds.resource_format, ds.title
            ORDER BY dataset_count DESC
            LIMIT 1
        ''', (tag_name, tag_name, f'%{tag_name}%'))
        
        result = cursor.fetchone()
        if not result:
            return jsonify({'error': 'Tag not found'}), 404
        
        agency, resource_format, title, dataset_count, available_count, unavailable_count, volatility = result
        
        # Determine category
        if agency == tag_name:
            category = 'agency'
            description = f'Datasets from {tag_name}'
        elif resource_format == tag_name:
            category = 'format'
            description = f'Datasets in {tag_name} format'
        else:
            category = 'topic'
            description = f'Datasets related to {tag_name.lower()}'
        
        # Get recent datasets for this tag
        cursor.execute('''
            SELECT 
                ds.dataset_id,
                ds.title,
                ds.agency,
                ds.availability,
                ds.created_at
            FROM dataset_states ds
            INNER JOIN (
                SELECT dataset_id, MAX(created_at) as max_created
                FROM dataset_states 
                GROUP BY dataset_id
            ) latest ON ds.dataset_id = latest.dataset_id 
            AND ds.created_at = latest.max_created
            WHERE (ds.agency = ? OR ds.resource_format = ? OR 
                   (ds.title LIKE ? AND ds.title IS NOT NULL))
            ORDER BY ds.created_at DESC
            LIMIT 20
        ''', (tag_name, tag_name, f'%{tag_name}%'))
        
        datasets = []
        for row in cursor.fetchall():
            datasets.append({
                'dataset_id': row[0],
                'title': row[1] or 'Untitled Dataset',
                'agency': row[2] or 'Unknown Agency',
                'availability': row[3] or 'unknown',
                'created_at': row[4]
            })
        
        conn.close()
        
        return jsonify({
            'name': tag_name,
            'category': category,
            'description': description,
            'dataset_count': dataset_count,
            'available_count': available_count,
            'unavailable_count': unavailable_count,
            'volatility': volatility or 0.5,
            'datasets': datasets
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/agencies')
def agencies_list():
    """Agencies list page"""
    return render_template('pages/agencies_list.html')

@app.route('/datasets/<dataset_id>')
def dataset_profile(dataset_id):
    """Dataset profile page"""
    return render_template('pages/dataset_profile.html')

@app.route('/agencies/<agency_name>')
def agency_page(agency_name):
    """Agency page"""
    return render_template('pages/agency_page.html')

# Additional missing routes
@app.route('/visualization')
def data_visualization():
    """Data visualization page"""
    return render_template('pages/data_visualization.html')

@app.route('/data-quality')
def data_quality():
    """Data quality page"""
    return render_template('pages/data_quality.html')

@app.route('/field-diff-panel')
def field_diff_panel():
    """Field diff panel page"""
    return render_template('pages/unified_app.html', page='field-diff-panel')

@app.route('/content-drift-charts')
def content_drift_charts():
    """Content drift charts page"""
    return render_template('components/content_drift_charts.html')

@app.route('/api/dataset/<dataset_id>/content-drift')
def api_dataset_content_drift(dataset_id):
    """Get content drift analysis for a dataset"""
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Get dataset information
        cursor.execute('''
            SELECT dataset_id, title, agency, url
            FROM dataset_states
            WHERE dataset_id = ?
            ORDER BY created_at DESC
            LIMIT 1
        ''', (dataset_id,))
        
        dataset_info = cursor.fetchone()
        if not dataset_info:
            return jsonify({'error': 'Dataset not found'}), 404
        
        # Get content drift data from dataset_states
        cursor.execute('''
            SELECT 
                DATE(created_at) as date,
                content_hash,
                row_count,
                column_count,
                file_size,
                created_at
            FROM dataset_states
            WHERE dataset_id = ?
            ORDER BY created_at DESC
            LIMIT 50
        ''', (dataset_id,))
        
        snapshots = cursor.fetchall()
        
        # Calculate content drift metrics
        drift_data = {
            'dataset_id': dataset_id,
            'title': dataset_info[1],
            'agency': dataset_info[2],
            'url': dataset_info[3],
            'snapshots': [],
            'statistics': {
                'total_snapshots': len(snapshots),
                'content_changes': 0,
                'avg_similarity': 0.0,
                'drift_score': 0.0
            },
            # Chart data in the format expected by JavaScript
            'timeline': [],
            'row_counts': [],
            'column_counts': [],
            'file_sizes': [],
            'similarities': [],
            'content_hashes': []
        }
        
        if len(snapshots) > 1:
            content_changes = 0
            similarity_scores = []
            
            for i in range(len(snapshots) - 1):
                current = snapshots[i]
                previous = snapshots[i + 1]
                
                # Calculate similarity based on content hash
                similarity = 1.0 if current[1] == previous[1] else 0.0
                similarity_scores.append(similarity)
                
                if current[1] != previous[1]:
                    content_changes += 1
                
                # Add to chart data arrays
                drift_data['timeline'].append(current[0])
                drift_data['row_counts'].append(current[2] or 0)
                drift_data['column_counts'].append(current[3] or 0)
                drift_data['file_sizes'].append(current[4] or 0)
                drift_data['similarities'].append(similarity)
                drift_data['content_hashes'].append(current[1] or '')
                
                drift_data['snapshots'].append({
                    'date': current[0],
                    'content_hash': current[1],
                    'row_count': current[2],
                    'column_count': current[3],
                    'file_size': current[4],
                    'created_at': current[5],
                    'similarity': similarity,
                    'has_content_change': current[1] != previous[1]
                })
            
            # Calculate statistics
            drift_data['statistics']['content_changes'] = content_changes
            drift_data['statistics']['avg_similarity'] = sum(similarity_scores) / len(similarity_scores) if similarity_scores else 0.0
            drift_data['statistics']['drift_score'] = 1.0 - drift_data['statistics']['avg_similarity']
        else:
            # Single snapshot - no drift possible
            current = snapshots[0]
            drift_data['timeline'].append(current[0])
            drift_data['row_counts'].append(current[2] or 0)
            drift_data['column_counts'].append(current[3] or 0)
            drift_data['file_sizes'].append(current[4] or 0)
            drift_data['similarities'].append(1.0)
            drift_data['content_hashes'].append(current[1] or '')
            
            drift_data['snapshots'] = [{
                'date': current[0],
                'content_hash': current[1],
                'row_count': current[2],
                'column_count': current[3],
                'file_size': current[4],
                'created_at': current[5],
                'similarity': 1.0,
                'has_content_change': False
            }]
        
        conn.close()
        return jsonify(drift_data)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/change-log-view')
def change_log_view():
    """Change log view page"""
    return render_template('pages/unified_app.html', page='change-log-view')

@app.route('/live-monitor')
def live_monitor():
    """Live monitoring dashboard page"""
    return render_template('pages/live_monitor.html')

@app.route('/postmortem-reports')
def postmortem_reports():
    """Post-mortem reports page"""
    return render_template('pages/postmortem_reports.html')

@app.route('/volatility-metrics')
def volatility_metrics():
    """Volatility metrics page"""
    return render_template('pages/volatility_metrics.html')

@app.route('/historian')
def historian():
    """Historian page"""
    return render_template('pages/unified_app.html', page='historian')

@app.route('/wayback')
def wayback():
    """Wayback page"""
    return render_template('pages/wayback.html')

@app.route('/api/wayback/timeline/<dataset_id>')
def api_wayback_timeline(dataset_id):
    """Get dataset timeline for wayback functionality"""
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Get dataset information
        cursor.execute('''
            SELECT dataset_id, title, agency, url
            FROM dataset_states
            WHERE dataset_id = ?
            ORDER BY created_at DESC
            LIMIT 1
        ''', (dataset_id,))
        
        dataset_info = cursor.fetchone()
        if not dataset_info:
            return jsonify({'error': 'Dataset not found'}), 404
        
        # Get timeline data
        cursor.execute('''
            SELECT 
                DATE(created_at) as date,
                availability as status,
                row_count,
                column_count,
                file_size,
                content_hash,
                created_at
            FROM dataset_states
            WHERE dataset_id = ?
            ORDER BY created_at DESC
        ''', (dataset_id,))
        
        timeline_data = cursor.fetchall()
        
        # Process timeline to detect changes
        timeline = []
        previous_snapshot = None
        
        for snapshot in timeline_data:
            date, status, row_count, column_count, file_size, content_hash, created_at = snapshot
            
            changes = []
            if previous_snapshot:
                prev_date, prev_status, prev_row_count, prev_column_count, prev_file_size, prev_content_hash, prev_created_at = previous_snapshot
                
                if prev_status != status:
                    changes.append(f"Status changed from {prev_status} to {status}")
                
                if prev_row_count != row_count:
                    if prev_row_count and row_count:
                        diff = row_count - prev_row_count
                        changes.append(f"Row count changed by {diff:+d} ({prev_row_count} → {row_count})")
                    elif row_count:
                        changes.append(f"Row count added: {row_count}")
                    elif prev_row_count:
                        changes.append(f"Row count removed: {prev_row_count}")
                
                if prev_column_count != column_count:
                    if prev_column_count and column_count:
                        diff = column_count - prev_column_count
                        changes.append(f"Column count changed by {diff:+d} ({prev_column_count} → {column_count})")
                    elif column_count:
                        changes.append(f"Column count added: {column_count}")
                    elif prev_column_count:
                        changes.append(f"Column count removed: {prev_column_count}")
                
                if prev_file_size != file_size:
                    if prev_file_size and file_size:
                        diff = file_size - prev_file_size
                        changes.append(f"File size changed by {diff:+d} bytes")
                    elif file_size:
                        changes.append(f"File size added: {file_size} bytes")
                    elif prev_file_size:
                        changes.append(f"File size removed: {prev_file_size} bytes")
                
                if prev_content_hash != content_hash:
                    changes.append("Content hash changed (data content modified)")
            
            timeline.append({
                'date': date,
                'status': status,
                'changes': changes,
                'has_changes': len(changes) > 0,
                'row_count': row_count,
                'column_count': column_count,
                'file_size': file_size,
                'created_at': created_at
            })
            
            previous_snapshot = snapshot
        
        conn.close()
        
        return jsonify({
            'dataset_id': dataset_id,
            'timeline': timeline,
            'total_snapshots': len(timeline),
            'change_events': sum(1 for item in timeline if item['has_changes'])
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/wayback/stats')
def api_wayback_stats():
    """Get wayback statistics"""
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Get total snapshots
        cursor.execute('SELECT COUNT(*) FROM dataset_states')
        total_snapshots = cursor.fetchone()[0]
        
        # Get datasets with snapshots
        cursor.execute('SELECT COUNT(DISTINCT dataset_id) FROM dataset_states')
        datasets_with_snapshots = cursor.fetchone()[0]
        
        # Get recent snapshots (last 7 days)
        cursor.execute('''
            SELECT COUNT(*) FROM dataset_states 
            WHERE created_at >= date('now', '-7 days')
        ''')
        recent_snapshots = cursor.fetchone()[0]
        
        # Get change events (datasets with different content hashes)
        cursor.execute('''
            SELECT COUNT(DISTINCT dataset_id) FROM dataset_states ds1
            WHERE EXISTS (
                SELECT 1 FROM dataset_states ds2 
                WHERE ds1.dataset_id = ds2.dataset_id 
                AND ds1.content_hash != ds2.content_hash
                AND ds1.created_at != ds2.created_at
            )
        ''')
        change_events = cursor.fetchone()[0]
        
        conn.close()
        
        return jsonify({
            'total_snapshots': total_snapshots,
            'datasets_with_snapshots': datasets_with_snapshots,
            'recent_snapshots': recent_snapshots,
            'change_events': change_events
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/wayback/changes/recent')
def api_wayback_recent_changes():
    """Get recent changes for wayback"""
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Get recent changes (last 30 days)
        cursor.execute('''
            SELECT 
                ds.dataset_id,
                ds.title,
                ds.agency,
                ds.availability as status,
                ds.row_count,
                ds.column_count,
                ds.created_at as date
            FROM dataset_states ds
            INNER JOIN (
                SELECT dataset_id, MAX(created_at) as max_created
                FROM dataset_states 
                WHERE created_at >= date('now', '-30 days')
                GROUP BY dataset_id
            ) latest ON ds.dataset_id = latest.dataset_id 
            AND ds.created_at = latest.max_created
            ORDER BY ds.created_at DESC
            LIMIT 50
        ''')
        
        changes = []
        for row in cursor.fetchall():
            changes.append({
                'dataset_id': row[0],
                'title': row[1] or f'Dataset {row[0][:8]}...',
                'agency': row[2] or 'Unknown Agency',
                'status': row[3] or 'unknown',
                'row_count': row[4],
                'column_count': row[5],
                'date': row[6]
            })
        
        conn.close()
        
        return jsonify({
            'changes': changes
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/wayback/compare/<dataset_id>/<from_date>/<to_date>')
def api_wayback_compare(dataset_id, from_date, to_date):
    """Compare two snapshots of a dataset"""
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Get from snapshot
        cursor.execute('''
            SELECT 
                created_at as date,
                availability as status,
                row_count,
                column_count,
                file_size,
                content_hash,
                title,
                agency
            FROM dataset_states
            WHERE dataset_id = ? AND DATE(created_at) <= ?
            ORDER BY created_at DESC
            LIMIT 1
        ''', (dataset_id, from_date))
        
        from_snapshot = cursor.fetchone()
        if not from_snapshot:
            return jsonify({'error': 'From snapshot not found'}), 404
        
        # Get to snapshot
        cursor.execute('''
            SELECT 
                created_at as date,
                availability as status,
                row_count,
                column_count,
                file_size,
                content_hash,
                title,
                agency
            FROM dataset_states
            WHERE dataset_id = ? AND DATE(created_at) <= ?
            ORDER BY created_at DESC
            LIMIT 1
        ''', (dataset_id, to_date))
        
        to_snapshot = cursor.fetchone()
        if not to_snapshot:
            return jsonify({'error': 'To snapshot not found'}), 404
        
        # Calculate differences
        from_date, from_status, from_row_count, from_column_count, from_file_size, from_content_hash, from_title, from_agency = from_snapshot
        to_date, to_status, to_row_count, to_column_count, to_file_size, to_content_hash, to_title, to_agency = to_snapshot
        
        differences = {
            'status_changed': from_status != to_status,
            'row_count_changed': from_row_count != to_row_count,
            'column_count_changed': from_column_count != to_column_count,
            'file_size_changed': from_file_size != to_file_size,
            'content_changed': from_content_hash != to_content_hash,
            'title_changed': from_title != to_title,
            'agency_changed': from_agency != to_agency,
            'row_count_difference': (to_row_count or 0) - (from_row_count or 0),
            'column_count_difference': (to_column_count or 0) - (from_column_count or 0),
            'file_size_difference': (to_file_size or 0) - (from_file_size or 0),
            'metadata_changes': {}
        }
        
        if from_title != to_title:
            differences['metadata_changes']['title'] = {'from': from_title, 'to': to_title}
        if from_agency != to_agency:
            differences['metadata_changes']['agency'] = {'from': from_agency, 'to': to_agency}
        
        conn.close()
        
        return jsonify({
            'dataset_id': dataset_id,
            'from_snapshot': {
                'date': from_date,
                'status': from_status,
                'row_count': from_row_count,
                'column_count': from_column_count,
                'file_size': from_file_size,
                'title': from_title,
                'agency': from_agency
            },
            'to_snapshot': {
                'date': to_date,
                'status': to_status,
                'row_count': to_row_count,
                'column_count': to_column_count,
                'file_size': to_file_size,
                'title': to_title,
                'agency': to_agency
            },
            'differences': differences
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/changes')
def changes():
    """Changes page"""
    return render_template('pages/unified_app.html', page='changes')


@app.route('/api-docs')
def api_docs():
    """API documentation page"""
    return render_template('pages/api_docs.html')

@app.route('/backup')
def backup():
    """Backup page"""
    return render_template('pages/backup.html')

@app.route('/search')
def search():
    """Search page"""
    return render_template('pages/search.html')

# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.route('/api/stats')
def api_stats():
    """API endpoint for monitoring statistics"""
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Get snapshot counts
        cursor.execute('SELECT COUNT(*) FROM dataset_states')
        total_snapshots = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(DISTINCT dataset_id) FROM dataset_states')
        unique_datasets = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM state_diffs')
        total_diffs = cursor.fetchone()[0]
        
        # Get availability stats from database
        availability_stats = {'available': 0, 'unavailable': 0, 'error': 0, 'unknown': 0}
        
        cursor.execute('''
            SELECT availability, COUNT(*) as count
            FROM dataset_states ds
            INNER JOIN (
                SELECT dataset_id, MAX(created_at) as max_created
                FROM dataset_states 
                GROUP BY dataset_id
            ) latest ON ds.dataset_id = latest.dataset_id 
            AND ds.created_at = latest.max_created
            GROUP BY availability
        ''')
        
        for row in cursor.fetchall():
            availability = row[0] or 'unknown'
            count = row[1]
            availability_stats[availability] = count
        
        conn.close()
        
        return jsonify({
            'total_snapshots': total_snapshots,
            'unique_datasets': unique_datasets,
            'total_diffs': total_diffs,
            'availability_stats': availability_stats,
            'last_updated': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/datasets')
def api_datasets():
    """API endpoint for all datasets with comprehensive state information"""
    try:
        # Get pagination parameters
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 1000, type=int)
        offset = (page - 1) * per_page
        
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # First, get the total count
        cursor.execute('''
            SELECT COUNT(DISTINCT ds.dataset_id)
            FROM dataset_states ds
            INNER JOIN (
                SELECT dataset_id, MAX(created_at) as max_created
                FROM dataset_states 
                GROUP BY dataset_id
            ) latest ON ds.dataset_id = latest.dataset_id 
            AND ds.created_at = latest.max_created
        ''')
        total_count = cursor.fetchone()[0]
        
        # Get datasets with enhanced data from dataset_states table and live_monitoring table
        cursor.execute('''
            SELECT ds.dataset_id, ds.snapshot_date, ds.title, ds.agency, ds.url,
                   ds.status_code, ds.content_hash, ds.file_size, ds.content_type,
                   ds.resource_format, ds.row_count, ds.column_count, ds.schema,
                   ds.last_modified, ds.availability, ds.created_at, 'enhanced_monitor' as source,
                   (SELECT COUNT(*) FROM dataset_states ds2 WHERE ds2.dataset_id = ds.dataset_id) as total_snapshots,
                   0 as total_diffs,
                   NULL as last_change,
                   lm.last_checked,
                   lm.status as monitoring_status,
                   lm.response_time_ms,
                   hs.license
            FROM dataset_states ds
            LEFT JOIN (
                SELECT dataset_id, last_checked, status, response_time_ms,
                       ROW_NUMBER() OVER (PARTITION BY dataset_id ORDER BY last_checked DESC) as rn
                FROM live_monitoring
            ) lm ON ds.dataset_id = lm.dataset_id AND lm.rn = 1
            LEFT JOIN (
                SELECT dataset_id, license,
                       ROW_NUMBER() OVER (PARTITION BY dataset_id ORDER BY created_at DESC) as rn
                FROM historian_snapshots
            ) hs ON ds.dataset_id = hs.dataset_id AND hs.rn = 1
            INNER JOIN (
                SELECT dataset_id, MAX(created_at) as max_created
                FROM dataset_states 
                GROUP BY dataset_id
            ) latest ON ds.dataset_id = latest.dataset_id 
            AND ds.created_at = latest.max_created
            ORDER BY lm.last_checked DESC NULLS LAST, ds.created_at DESC
            LIMIT ? OFFSET ?
        ''', (per_page, offset))
        
        columns = [description[0] for description in cursor.description]
        datasets = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        # Add volatility metrics for each dataset
        for dataset in datasets:
            dataset_id = dataset['dataset_id']
            
            # Set default volatility values
            dataset['avg_volatility'] = 0.0
            dataset['max_volatility'] = 0.0
            dataset['volatility_measurements'] = 0
            dataset['recent_changes'] = []
            
            # Format monitoring data
            if dataset['last_checked']:
                # Format last_checked as readable date
                try:
                    from datetime import datetime
                    last_checked_dt = datetime.fromisoformat(dataset['last_checked'].replace('Z', '+00:00'))
                    dataset['last_checked'] = last_checked_dt.strftime('%Y-%m-%d %H:%M:%S')
                except:
                    dataset['last_checked'] = str(dataset['last_checked'])
            else:
                dataset['last_checked'] = 'N/A'
            
            if dataset['response_time_ms']:
                # Format response time as readable format
                if dataset['response_time_ms'] < 1000:
                    dataset['response_time'] = f"{dataset['response_time_ms']}ms"
                else:
                    dataset['response_time'] = f"{dataset['response_time_ms']/1000:.1f}s"
            else:
                dataset['response_time'] = 'N/A'
            
            # Use monitoring status if available, otherwise use availability
            if dataset['monitoring_status']:
                dataset['status'] = dataset['monitoring_status']
            else:
                # Determine status based on availability and recent activity
                if dataset['availability'] == 'available':
                    if dataset['total_diffs'] > 0:
                        dataset['status'] = 'active'
                    else:
                        dataset['status'] = 'stable'
                elif dataset['availability'] == 'partially_available':
                    dataset['status'] = 'degraded'
                else:
                    dataset['status'] = 'unavailable'
        
        conn.close()
        
        # Calculate pagination info
        total_pages = (total_count + per_page - 1) // per_page
        
        return jsonify({
            'datasets': datasets,
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total_count': total_count,
                'total_pages': total_pages,
                'has_next': page < total_pages,
                'has_prev': page > 1
            }
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/dataset/<dataset_id>')
def api_dataset_detail(dataset_id):
    """API endpoint for specific dataset details"""
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Get timeline
        cursor.execute('''
            SELECT snapshot_date, row_count, column_count, file_size, created_at
            FROM dataset_states 
            WHERE dataset_id = ?
            ORDER BY snapshot_date ASC
        ''', (dataset_id,))
        
        columns = [description[0] for description in cursor.description]
        timeline = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        # Get diffs
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
        
        return jsonify({
            'dataset_id': dataset_id,
            'timeline': timeline,
            'diffs': diffs
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/vanished')
def api_vanished():
    """API endpoint for vanished datasets"""
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Get vanished datasets (datasets with availability issues)
        cursor.execute('''
            SELECT DISTINCT ds.dataset_id, ds.title, ds.agency, ds.snapshot_date, ds.availability, ds.status_code, ds.url
            FROM dataset_states ds
            WHERE ds.availability = 'unavailable' OR ds.status_code >= 400
            ORDER BY ds.snapshot_date DESC
        ''')
        
        vanished_datasets = []
        for row in cursor.fetchall():
            status = 'REMOVED' if row[4] == 'unavailable' else 'ERROR'
            dataset_url = row[6] if row[6] else ''
            
            # Generate archive URL
            archive_url = f'/archive/{row[0]}'
            
            vanished_datasets.append({
                'dataset_id': row[0],
                'title': row[1],
                'agency': row[2],
                'last_seen': row[3],
                'status': status,
                'archive_url': archive_url,
                'original_url': dataset_url
            })
        
        conn.close()
        
        return jsonify({
            'vanished_datasets': vanished_datasets,
            'total_count': len(vanished_datasets)
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/archive/<dataset_id>')
def archive_view(dataset_id):
    """Archive view for a specific dataset"""
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Get dataset information
        cursor.execute('''
            SELECT dataset_id, title, agency, url, snapshot_date, availability, status_code
            FROM dataset_states
            WHERE dataset_id = ?
            ORDER BY snapshot_date DESC
            LIMIT 1
        ''', (dataset_id,))
        
        dataset_info = cursor.fetchone()
        if not dataset_info:
            return render_template('pages/archive_not_found.html', dataset_id=dataset_id)
        
        # Get historical snapshots
        cursor.execute('''
            SELECT snapshot_date, availability, status_code, content_hash, file_size
            FROM dataset_states
            WHERE dataset_id = ?
            ORDER BY snapshot_date DESC
            LIMIT 10
        ''', (dataset_id,))
        
        snapshots = cursor.fetchall()
        
        # Get monitoring history
        cursor.execute('''
            SELECT last_checked, status, response_time_ms, change_detected
            FROM live_monitoring
            WHERE dataset_id = ?
            ORDER BY last_checked DESC
            LIMIT 10
        ''', (dataset_id,))
        
        monitoring_history = cursor.fetchall()
        
        conn.close()
        
        # Generate archive URLs
        dataset_url = dataset_info[3] if dataset_info[3] else ''
        wayback_url = f"https://web.archive.org/web/*/{dataset_url}" if dataset_url else None
        archive_url = f"https://web.archive.org/web/{dataset_info[4]}/{dataset_url}" if dataset_url and dataset_info[4] else wayback_url
        
        return render_template('pages/archive_detail.html', 
                             dataset_info=dataset_info,
                             snapshots=snapshots,
                             monitoring_history=monitoring_history,
                             wayback_url=wayback_url,
                             archive_url=archive_url)
        
    except Exception as e:
        return render_template('pages/archive_error.html', error=str(e))

@app.route('/api/dataset/<dataset_id>')
def api_dataset_details(dataset_id):
    """API endpoint for individual dataset details"""
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Get dataset information
        cursor.execute('''
            SELECT dataset_id, title, agency, url, snapshot_date, availability, status_code,
                   content_hash, file_size, content_type, resource_format, row_count, column_count,
                   schema, last_modified, created_at
            FROM dataset_states
            WHERE dataset_id = ?
            ORDER BY snapshot_date DESC
            LIMIT 1
        ''', (dataset_id,))
        
        dataset_info = cursor.fetchone()
        if not dataset_info:
            return jsonify({'error': 'Dataset not found'}), 404
        
        # Get historical snapshots
        cursor.execute('''
            SELECT snapshot_date, availability, status_code, content_hash, file_size, row_count, column_count
            FROM dataset_states
            WHERE dataset_id = ?
            ORDER BY snapshot_date DESC
            LIMIT 10
        ''', (dataset_id,))
        
        snapshots = cursor.fetchall()
        
        # Get monitoring history
        cursor.execute('''
            SELECT last_checked, status, response_time_ms, change_detected
            FROM live_monitoring
            WHERE dataset_id = ?
            ORDER BY last_checked DESC
            LIMIT 10
        ''', (dataset_id,))
        
        monitoring_history = cursor.fetchall()
        
        conn.close()
        
        # Format the response
        dataset = {
            'dataset_id': dataset_info[0],
            'title': dataset_info[1],
            'agency': dataset_info[2],
            'url': dataset_info[3],
            'snapshot_date': dataset_info[4],
            'availability': dataset_info[5],
            'status_code': dataset_info[6],
            'content_hash': dataset_info[7],
            'file_size': dataset_info[8],
            'content_type': dataset_info[9],
            'resource_format': dataset_info[10],
            'row_count': dataset_info[11],
            'column_count': dataset_info[12],
            'schema': dataset_info[13],
            'last_modified': dataset_info[14],
            'created_at': dataset_info[15]
        }
        
        timeline = []
        for snapshot in snapshots:
            timeline.append({
                'date': snapshot[0],
                'availability': snapshot[1],
                'status_code': snapshot[2],
                'content_hash': snapshot[3],
                'file_size': snapshot[4],
                'row_count': snapshot[5],
                'column_count': snapshot[6]
            })
        
        diffs = []
        for monitoring in monitoring_history:
            if monitoring[3]:  # change_detected
                diffs.append({
                    'date': monitoring[0],
                    'status': monitoring[1],
                    'response_time_ms': monitoring[2]
                })
        
        return jsonify({
            'dataset': dataset,
            'timeline': timeline,
            'diffs': diffs
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/monitoring/stats')
def api_monitoring_stats():
    """API endpoint for monitoring system statistics"""
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Get live monitoring status breakdown (excluding rate-limited datasets)
        cursor.execute('''
            SELECT status, COUNT(*) as count
            FROM live_monitoring
            WHERE status != 'rate_limited' AND status != '429'
            GROUP BY status
            ORDER BY count DESC
        ''')
        
        monitoring_stats = {}
        total_checks = 0
        for row in cursor.fetchall():
            monitoring_stats[row[0]] = row[1]
            total_checks += row[1]
        
        # Get status code breakdown for unavailable datasets (excluding 429 rate limited)
        cursor.execute('''
            SELECT status_code, COUNT(*) as count
            FROM dataset_states
            WHERE availability = 'unavailable' AND status_code != 429
            GROUP BY status_code
            ORDER BY count DESC
        ''')
        
        status_codes = {}
        for row in cursor.fetchall():
            status_codes[str(row[0])] = row[1]
        
        # Get last monitoring time
        cursor.execute('''
            SELECT MAX(last_checked) as last_check
            FROM live_monitoring
        ''')
        
        last_check_row = cursor.fetchone()
        last_check = last_check_row[0] if last_check_row and last_check_row[0] else None
        
        # Get rate limiting statistics
        try:
            from monitoring.rate_limiter import rate_limiter
            rate_limiting_stats = rate_limiter.get_rate_limit_stats()
        except Exception as e:
            rate_limiting_stats = {'error': str(e)}
        
        conn.close()
        
        return jsonify({
            'monitoring_stats': monitoring_stats,
            'total_checks': total_checks,
            'status_codes': status_codes,
            'last_check': last_check,
            'system_status': 'operational' if total_checks > 0 else 'inactive',
            'rate_limiting': rate_limiting_stats
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/discovery/start', methods=['POST'])
def start_discovery():
    """Start comprehensive dataset discovery"""
    try:
        from core.comprehensive_discovery import ComprehensiveDiscovery
        
        discovery = ComprehensiveDiscovery()
        
        # Run discovery in background
        import asyncio
        import threading
        
        def run_discovery():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(discovery.run_comprehensive_discovery(
                f"api_discovery_{int(time.time())}"
            ))
            loop.close()
        
        thread = threading.Thread(target=run_discovery)
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'status': 'started',
            'message': 'Comprehensive discovery started in background'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/discovery/status')
def discovery_status():
    """Get discovery status and statistics"""
    try:
        from core.comprehensive_discovery import ComprehensiveDiscovery
        
        discovery = ComprehensiveDiscovery()
        stats = discovery.get_discovery_stats()
        
        return jsonify(stats)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/monitoring/start', methods=['POST'])
def start_monitoring():
    """Start comprehensive monitoring"""
    try:
        from monitoring.comprehensive_scheduler import ComprehensiveScheduler
        
        scheduler = ComprehensiveScheduler()
        
        # Start monitoring in background
        import asyncio
        import threading
        
        def run_monitoring():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(scheduler.start_monitoring())
            loop.close()
        
        thread = threading.Thread(target=run_monitoring)
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'status': 'started',
            'message': 'Comprehensive monitoring started in background'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/monitoring/status')
def monitoring_status():
    """Get monitoring status and statistics"""
    try:
        from monitoring.comprehensive_scheduler import ComprehensiveScheduler
        
        scheduler = ComprehensiveScheduler()
        status = scheduler.get_monitoring_status()
        
        return jsonify(status)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/monitoring/init', methods=['POST'])
def init_monitoring():
    """Initialize monitoring schedule for all datasets"""
    try:
        from monitoring.comprehensive_scheduler import ComprehensiveScheduler
        
        scheduler = ComprehensiveScheduler()
        
        # Initialize schedule in background
        import asyncio
        import threading
        
        def run_init():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            count = loop.run_until_complete(scheduler.initialize_monitoring_schedule())
            loop.close()
            return count
        
        thread = threading.Thread(target=run_init)
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'status': 'initializing',
            'message': 'Monitoring schedule initialization started in background'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/logs')
def api_logs():
    """API endpoint for system logs"""
    try:
        log_files = [
            'historian.log',
            'daily_monitoring.log', 
            'comprehensive_analysis.log',
            'diff_generation.log'
        ]
        
        logs = []
        for log_file in log_files:
            log_path = Path(log_file)
            if log_path.exists():
                with open(log_path, 'r') as f:
                    lines = f.readlines()
                    # Get last 100 lines
                    recent_lines = lines[-100:] if len(lines) > 100 else lines
                    logs.append({
                        'file': log_file,
                        'lines': recent_lines,
                        'total_lines': len(lines)
                    })
        
        return jsonify(logs)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/system/status')
def api_system_status():
    """API endpoint for comprehensive system status"""
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Initialize default values
        total_datasets = 0
        total_snapshots = 0
        total_diffs = 0
        availability_stats = {}
        recent_snapshots = 0
        recent_diffs = 0
        volatile_datasets = []
        successful_requests = 0
        failed_requests = 0
        
        try:
            # Get comprehensive statistics
            cursor.execute('SELECT COUNT(DISTINCT dataset_id) FROM dataset_states')
            total_datasets = cursor.fetchone()[0] or 0
            
            cursor.execute('SELECT COUNT(*) FROM dataset_states')
            total_snapshots = cursor.fetchone()[0] or 0
        except Exception as e:
            print(f"Error getting basic stats: {e}")
        
        try:
            # Check if state_diffs table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='state_diffs'")
            state_diffs_exists = cursor.fetchone() is not None
            
            if state_diffs_exists:
                cursor.execute('SELECT COUNT(*) FROM state_diffs')
                total_diffs = cursor.fetchone()[0] or 0
        except Exception as e:
            print(f"Error checking state_diffs: {e}")
        
        try:
            # Get availability breakdown
            cursor.execute('''
                SELECT availability, COUNT(*) as count
                FROM dataset_states ds
                INNER JOIN (
                    SELECT dataset_id, MAX(created_at) as max_created
                    FROM dataset_states 
                    GROUP BY dataset_id
                ) latest ON ds.dataset_id = latest.dataset_id 
                AND ds.created_at = latest.max_created
                GROUP BY availability
            ''')
            availability_stats = dict(cursor.fetchall())
        except Exception as e:
            print(f"Error getting availability stats: {e}")
            availability_stats = {'available': 0, 'unavailable': 0, 'partially_available': 0}
        
        try:
            # Get recent activity (last 24 hours)
            cursor.execute('''
                SELECT COUNT(*) FROM dataset_states 
                WHERE created_at >= datetime('now', '-1 day')
            ''')
            recent_snapshots = cursor.fetchone()[0] or 0
        except Exception as e:
            print(f"Error getting recent snapshots: {e}")
        
        try:
            if state_diffs_exists:
                cursor.execute('''
                    SELECT COUNT(*) FROM state_diffs 
                    WHERE created_at >= datetime('now', '-1 day')
                ''')
                recent_diffs = cursor.fetchone()[0] or 0
                
                # Get top volatile datasets
                cursor.execute('''
                    SELECT dataset_id, AVG(volatility_score) as avg_volatility
                    FROM state_diffs 
                    GROUP BY dataset_id
                    HAVING avg_volatility > 0.5
                    ORDER BY avg_volatility DESC
                    LIMIT 10
                ''')
                volatile_datasets = [{'dataset_id': row[0], 'volatility': round(row[1], 3)} for row in cursor.fetchall()]
        except Exception as e:
            print(f"Error getting diff stats: {e}")
        
        try:
            # Get processing status
            cursor.execute('''
                SELECT COUNT(*) FROM dataset_states 
                WHERE status_code >= 200 AND status_code < 300
            ''')
            successful_requests = cursor.fetchone()[0] or 0
            
            cursor.execute('''
                SELECT COUNT(*) FROM dataset_states 
                WHERE status_code >= 400
            ''')
            failed_requests = cursor.fetchone()[0] or 0
        except Exception as e:
            print(f"Error getting request stats: {e}")
        
        conn.close()
        
        # Calculate success rate safely
        total_requests = successful_requests + failed_requests
        success_rate = round(successful_requests / total_requests * 100, 2) if total_requests > 0 else 0
        
        # Get agencies count
        try:
            cursor.execute('SELECT COUNT(DISTINCT agency) FROM dataset_states WHERE agency IS NOT NULL AND agency != ""')
            total_agencies = cursor.fetchone()[0] or 0
        except Exception as e:
            print(f"Error getting agencies count: {e}")
            total_agencies = 0
        
        # Get performance metrics
        try:
            cursor.execute('SELECT AVG(response_time_ms) FROM dataset_states WHERE response_time_ms IS NOT NULL')
            avg_response_time = cursor.fetchone()[0] or 0
        except Exception as e:
            print(f"Error getting response time: {e}")
            avg_response_time = 0
        
        return jsonify({
            'total_datasets': total_datasets,
            'total_snapshots': total_snapshots,
            'total_diffs': total_diffs,
            'availability_stats': availability_stats,
            'recent_activity': {
                'snapshots_24h': recent_snapshots,
                'diffs_24h': recent_diffs
            },
            'volatile_datasets': volatile_datasets,
            'request_stats': {
                'successful': successful_requests,
                'failed': failed_requests,
                'success_rate': success_rate
            },
            'agencies': {
                'total': total_agencies
            },
            'performance': {
                'avg_response_time_ms': round(avg_response_time, 2)
            },
            'monitoring_status': 'running' if monitoring_thread and monitoring_thread.is_alive() else 'stopped',
            'last_updated': datetime.now().isoformat()
        })
    except Exception as e:
        print(f"System status error: {e}")
        return jsonify({
            'error': str(e),
            'total_datasets': 0,
            'total_snapshots': 0,
            'total_diffs': 0,
            'availability_stats': {},
            'recent_activity': {'snapshots_24h': 0, 'diffs_24h': 0},
            'volatile_datasets': [],
            'request_stats': {'successful': 0, 'failed': 0, 'success_rate': 0},
            'last_updated': datetime.now().isoformat()
        }), 500

@app.route('/api/dataset/<dataset_id>/data')
def api_dataset_data(dataset_id):
    """API endpoint for dataset data content"""
    try:
        snapshot_date = request.args.get('snapshot_date')
        
        dataset_states_dir = Path(f"dataset_states/{dataset_id}")
        
        if not dataset_states_dir.exists():
            return jsonify({'error': 'Dataset not found'}), 404
        
        # Get latest snapshot if no date specified
        if not snapshot_date:
            snapshot_dirs = [d for d in dataset_states_dir.iterdir() if d.is_dir()]
            if not snapshot_dirs:
                return jsonify({'error': 'No snapshots found'}), 404
            snapshot_date = max(snapshot_dirs, key=lambda x: x.name).name
        
        snapshot_dir = dataset_states_dir / snapshot_date
        
        # Look for data files
        data_files = []
        for file_path in snapshot_dir.iterdir():
            if file_path.is_file() and file_path.name != 'metadata.json':
                data_files.append(file_path)
        
        if not data_files:
            return jsonify({
                'dataset_id': dataset_id,
                'snapshot_date': snapshot_date,
                'data_files': [],
                'error': 'No data files found'
            })
        
        # Try to load the first data file
        data_file = data_files[0]
        file_extension = data_file.suffix.lower()
        
        try:
            if file_extension == '.csv':
                import pandas as pd
                df = pd.read_csv(data_file, nrows=1000)  # Limit to first 1000 rows
                data_content = {
                    'type': 'csv',
                    'columns': df.columns.tolist(),
                    'dtypes': df.dtypes.to_dict(),
                    'shape': df.shape,
                    'sample_data': df.head(10).to_dict('records'),
                    'file_size': data_file.stat().st_size,
                    'file_name': data_file.name
                }
            elif file_extension == '.json':
                with open(data_file, 'r') as f:
                    json_data = json.load(f)
                
                if isinstance(json_data, list) and len(json_data) > 0:
                    import pandas as pd
                    df = pd.DataFrame(json_data[:1000])  # Limit to first 1000 items
                    data_content = {
                        'type': 'json',
                        'columns': df.columns.tolist(),
                        'dtypes': df.dtypes.to_dict(),
                        'shape': df.shape,
                        'sample_data': df.head(10).to_dict('records'),
                        'file_size': data_file.stat().st_size,
                        'file_name': data_file.name
                    }
                else:
                    data_content = {
                        'type': 'json',
                        'raw_data': json_data,
                        'file_size': data_file.stat().st_size,
                        'file_name': data_file.name
                    }
            else:
                # For other file types, show basic info
                with open(data_file, 'rb') as f:
                    content = f.read(1000)  # First 1000 bytes
                
                data_content = {
                    'type': 'binary',
                    'file_extension': file_extension,
                    'file_size': data_file.stat().st_size,
                    'file_name': data_file.name,
                    'preview': content.decode('utf-8', errors='ignore')
                }
            
            return jsonify({
                'dataset_id': dataset_id,
                'snapshot_date': snapshot_date,
                'data_files': [f.name for f in data_files],
                'data_content': data_content
            })
            
        except Exception as e:
            return jsonify({
                'dataset_id': dataset_id,
                'snapshot_date': snapshot_date,
                'data_files': [f.name for f in data_files],
                'error': f'Error loading data: {str(e)}'
            })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/alerts')
def api_alerts():
    """API endpoint for active alerts"""
    try:
        alerts = monitor.get_active_alerts()
        
        # If no alerts from change_alerts table, create alerts from recent changes
        if not alerts:
            conn = get_database_connection()
            cursor = conn.cursor()
            
            # Get recent changes and convert them to alerts
            cursor.execute('''
                SELECT ds.dataset_id, ds.title, ds.agency, ds.created_at, ds.availability
                FROM dataset_states ds
                INNER JOIN (
                    SELECT dataset_id, MAX(created_at) as max_created
                    FROM dataset_states 
                    GROUP BY dataset_id
                ) latest ON ds.dataset_id = latest.dataset_id 
                AND ds.created_at = latest.max_created
                WHERE ds.created_at >= datetime('now', '-24 hours')
                ORDER BY ds.created_at DESC
                LIMIT 10
            ''')
            
            recent_datasets = cursor.fetchall()
            conn.close()
            
            # Convert recent changes to alert format
            alerts = []
            for i, (dataset_id, title, agency, created_at, availability) in enumerate(recent_datasets):
                # Fix timestamp formatting - ensure it's current time if it's in the future
                timestamp = created_at
                try:
                    from datetime import datetime
                    parsed_time = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    current_time = datetime.now()
                    
                    # If timestamp is in the future, use current time instead
                    if parsed_time > current_time:
                        timestamp = current_time.strftime('%Y-%m-%d %H:%M:%S')
                except:
                    # If parsing fails, use current time
                    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                alert_type = 'content_change' if availability == 'available' else 'availability_change'
                severity = 'medium' if availability == 'available' else 'high'
                
                alerts.append({
                    'id': f'change_{i}',
                    'dataset_id': dataset_id,
                    'title': title or f'Dataset {dataset_id[:8]}...',
                    'agency': agency or 'Unknown Agency',
                    'message': f'Dataset {title or dataset_id[:8]}... has changed ({alert_type})',
                    'enhanced_message': f'{title or f"Dataset {dataset_id[:8]}..."} ({dataset_id[:8]}...)',
                    'dataset_name': title or f'Dataset {dataset_id[:8]}...',
                    'agency_name': agency or 'Unknown Agency',
                    'severity': severity,
                    'created_at': timestamp,
                    'alert_type': alert_type
                })
        
        return jsonify(alerts)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/alerts/<int:alert_id>/acknowledge', methods=['POST'])
def acknowledge_alert(alert_id):
    """Acknowledge an alert"""
    try:
        monitor.acknowledge_alert(alert_id)
        return jsonify({'status': 'success'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/stop_monitoring', methods=['POST'])
def stop_monitoring():
    """Stop continuous monitoring"""
    try:
        monitor.stop_monitoring()
        return jsonify({'status': 'stopped'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/run_cycle', methods=['POST'])
def run_cycle():
    """Run a single monitoring cycle"""
    try:
        def run_single_cycle():
            import asyncio
            asyncio.run(monitor.monitor_all_datasets())
        
        cycle_thread = threading.Thread(target=run_single_cycle, daemon=True)
        cycle_thread.start()
        
        return jsonify({'status': 'cycle_started'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/health')
def api_health():
    """API endpoint for system health"""
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Check recent activity
        cursor.execute('''
            SELECT COUNT(*) FROM live_monitoring 
            WHERE last_checked > datetime('now', '-1 hour')
        ''')
        recent_checks = cursor.fetchone()[0]
        
        # Check for errors
        cursor.execute('''
            SELECT COUNT(*) FROM live_monitoring 
            WHERE last_checked > datetime('now', '-1 hour')
            AND status = 'error'
        ''')
        recent_errors = cursor.fetchone()[0]
        
        # Check monitoring thread status
        monitoring_status = "running" if (monitoring_thread and monitoring_thread.is_alive()) else "stopped"
        
        health_score = 100
        if recent_checks == 0:
            health_score -= 50
        if recent_errors > recent_checks * 0.1:  # More than 10% errors
            health_score -= 30
        if monitoring_status == "stopped":
            health_score -= 20
        
        conn.close()
        
        return jsonify({
            'health_score': max(0, health_score),
            'recent_checks': recent_checks,
            'recent_errors': recent_errors,
            'monitoring_status': monitoring_status,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/timeline')
def api_timeline():
    """API endpoint for timeline data and charts"""
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Get basic timeline statistics from existing tables
        cursor.execute('''
            SELECT COUNT(DISTINCT dataset_id) as total_datasets
            FROM dataset_states
        ''')
        total_datasets = cursor.fetchone()[0]
        
        cursor.execute('''
            SELECT COUNT(DISTINCT dataset_id) as available_datasets
            FROM dataset_states
            WHERE availability = 'available'
        ''')
        available_datasets = cursor.fetchone()[0]
        
        cursor.execute('''
            SELECT COUNT(DISTINCT dataset_id) as unavailable_datasets
            FROM dataset_states
            WHERE availability = 'unavailable'
        ''')
        unavailable_datasets = cursor.fetchone()[0]
        
        cursor.execute('''
            SELECT COUNT(*) as total_changes
            FROM live_monitoring
            WHERE change_detected = 1
        ''')
        total_changes = cursor.fetchone()[0]
        
        cursor.execute('''
            SELECT MAX(last_checked) as last_updated
            FROM live_monitoring
        ''')
        last_updated = cursor.fetchone()[0]
        
        conn.close()
        
        # Create summary data
        summary = {
            'total_datasets': total_datasets,
            'available_datasets': available_datasets,
            'unavailable_datasets': unavailable_datasets,
            'total_changes': total_changes,
            'last_updated': last_updated
        }
        
        # Create simple chart data
        charts = {
            'dataset_count': {
                'type': 'line',
                'data': {
                    'labels': ['Total', 'Available', 'Unavailable'],
                    'datasets': [{
                        'label': 'Dataset Count',
                        'data': [total_datasets, available_datasets, unavailable_datasets],
                        'backgroundColor': ['#4a90e2', '#7ed321', '#f5a623']
                    }]
                }
            }
        }
        
        return jsonify({
            'charts': charts,
            'summary': summary,
            'days': 30,
            'agency': None
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/timeline/monthly')
def api_timeline_monthly():
    """API endpoint for monthly timeline data and charts"""
    try:
        months = request.args.get('months', 12, type=int)
        agency = request.args.get('agency', None)
        charts = timeline_visualizer.generate_monthly_timeline_charts(months, agency)
        summary = timeline_visualizer.generate_monthly_summary_stats(months, agency)
        
        return jsonify({
            'charts': charts,
            'summary': summary,
            'months': months,
            'agency': agency
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/changes')
def api_changes():
    """API endpoint for recent changes"""
    try:
        limit = request.args.get('limit', 10, type=int)
        hours = request.args.get('hours', 24, type=int)
        
        # Get changes from the last N hours
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Check if dataset_changes table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='dataset_changes'")
        changes_table_exists = cursor.fetchone() is not None
        
        changes = []
        if changes_table_exists:
            cursor.execute('''
                SELECT dc.dataset_id, dc.created_at, dc.change_type, dc.change_description,
                       dc.severity, dc.old_value, dc.new_value, dt.title, dt.agency
                FROM dataset_changes dc
                LEFT JOIN dataset_timeline dt ON dc.dataset_id = dt.dataset_id 
                    AND dc.change_date = dt.snapshot_date
                WHERE dc.created_at >= datetime('now', '-{} hours')
                ORDER BY dc.created_at DESC
                LIMIT ?
            '''.format(hours), (limit,))
            
            for row in cursor.fetchall():
                # Fix timestamp formatting - ensure it's current time if it's in the future
                timestamp = row[1]
                try:
                    # Parse the timestamp and check if it's in the future
                    from datetime import datetime
                    parsed_time = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    current_time = datetime.now()
                    
                    # If timestamp is in the future, use current time instead
                    if parsed_time > current_time:
                        timestamp = current_time.strftime('%Y-%m-%d %H:%M:%S')
                except:
                    # If parsing fails, use current time
                    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                changes.append({
                    'dataset_id': row[0],
                    'date': timestamp,
                    'type': row[2],
                    'description': row[3],
                    'severity': row[4],
                    'old_value': row[5],
                    'new_value': row[6],
                    'title': row[7] or 'Unknown',
                    'agency': row[8] or 'Unknown'
                })
        
        conn.close()
        
        return jsonify({
            'changes': changes,
            'count': len(changes)
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/dataset/<dataset_id>/timeline')
def api_dataset_timeline(dataset_id):
    """API endpoint for specific dataset timeline"""
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Get dataset data from dataset_states (more complete)
        cursor.execute('''
            SELECT snapshot_date, title, agency, availability, row_count, column_count,
                   file_size, resource_format, status_code, created_at
            FROM dataset_states 
            WHERE dataset_id = ?
            ORDER BY created_at ASC
        ''', (dataset_id,))
        
        timeline = []
        for row in cursor.fetchall():
            timeline.append({
                'date': row[0],
                'title': row[1],
                'agency': row[2],
                'availability': row[3],
                'row_count': row[4] or 0,
                'column_count': row[5] or 0,
                'file_size': row[6] or 0,
                'resource_format': row[7] or 'Unknown',
                'status_code': row[8] or 0
            })
        
        conn.close()
        
        return jsonify({
            'dataset_id': dataset_id,
            'timeline': timeline,
            'count': len(timeline)
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/snapshot', methods=['POST'])
def api_create_snapshot():
    """API endpoint to create a manual snapshot"""
    try:
        snapshot_data = time_series_manager.create_daily_snapshot()
        changes = time_series_manager.detect_changes()
        
        return jsonify({
            'snapshot': snapshot_data,
            'changes': changes,
            'success': True
        })
        
    except Exception as e:
        return jsonify({'error': str(e), 'success': False}), 500

@app.route('/api/agencies')
def api_agencies():
    """API endpoint for agency comparison data"""
    try:
        days = request.args.get('days', 30, type=int)
        comparison_data = agency_analytics.get_agency_comparison(days)
        
        return jsonify(comparison_data)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/agencies/leaderboard')
def api_agency_leaderboard():
    """API endpoint for agency leaderboard"""
    try:
        metric = request.args.get('metric', 'total_datasets')
        limit = request.args.get('limit', 10, type=int)
        leaderboard = agency_analytics.get_agency_leaderboard(metric, limit)
        
        return jsonify({
            'leaderboard': leaderboard,
            'metric': metric,
            'limit': limit
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/agencies/<agency_name>')
def api_agency_details(agency_name):
    """API endpoint for specific agency details"""
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Decode URL-encoded agency name
        agency_name = agency_name.replace('%20', ' ').replace('%2C', ',')
        
        # Get agency overview
        cursor.execute('''
            SELECT 
                COUNT(DISTINCT ds.dataset_id) as total_datasets,
                COUNT(DISTINCT CASE WHEN ds.availability = 'available' THEN ds.dataset_id END) as available_datasets,
                COUNT(DISTINCT CASE WHEN ds.availability = 'unavailable' THEN ds.dataset_id END) as vanished_datasets,
                AVG(ds.row_count) as avg_rows,
                AVG(ds.column_count) as avg_columns,
                SUM(ds.file_size) as total_file_size,
                COUNT(DISTINCT ds.resource_format) as format_diversity
            FROM dataset_states ds
            INNER JOIN (
                SELECT dataset_id, MAX(created_at) as max_created
                FROM dataset_states 
                GROUP BY dataset_id
            ) latest ON ds.dataset_id = latest.dataset_id 
            AND ds.created_at = latest.max_created
            WHERE ds.agency = ?
        ''', (agency_name,))
        
        overview = cursor.fetchone()
        if not overview:
            return jsonify({'error': 'Agency not found'}), 404
        
        total_datasets, available_datasets, vanished_datasets, avg_rows, avg_cols, total_size, format_diversity = overview
        
        # Get volatility metrics (using a simple calculation for now)
        cursor.execute('''
            SELECT AVG(CASE 
                WHEN ds.row_count IS NOT NULL AND ds.column_count IS NOT NULL 
                THEN (ds.row_count * ds.column_count) / 1000000.0 
                ELSE 0 
            END) as avg_volatility
            FROM dataset_states ds
            INNER JOIN (
                SELECT dataset_id, MAX(created_at) as max_created
                FROM dataset_states 
                GROUP BY dataset_id
            ) latest ON ds.dataset_id = latest.dataset_id 
            AND ds.created_at = latest.max_created
            WHERE ds.agency = ?
        ''', (agency_name,))
        
        volatility_row = cursor.fetchone()
        avg_volatility = volatility_row[0] or 0.0
        
        # Get monthly trends
        cursor.execute('''
            SELECT 
                strftime('%Y-%m', ds.created_at) as month,
                COUNT(DISTINCT ds.dataset_id) as new_datasets
            FROM dataset_states ds
            WHERE ds.agency = ?
            GROUP BY strftime('%Y-%m', ds.created_at)
            ORDER BY month DESC
            LIMIT 12
        ''', (agency_name,))
        
        monthly_trends = [
            {'month': row[0], 'new_datasets': row[1]}
            for row in cursor.fetchall()
        ]
        
        # Get most volatile datasets
        cursor.execute('''
            SELECT 
                ds.dataset_id,
                ds.title,
                (ds.row_count * ds.column_count) / 1000000.0 as volatility
            FROM dataset_states ds
            INNER JOIN (
                SELECT dataset_id, MAX(created_at) as max_created
                FROM dataset_states 
                GROUP BY dataset_id
            ) latest ON ds.dataset_id = latest.dataset_id 
            AND ds.created_at = latest.max_created
            WHERE ds.agency = ? AND ds.availability = 'available'
            ORDER BY volatility DESC
            LIMIT 10
        ''', (agency_name,))
        
        most_volatile_datasets = [
            {
                'dataset_id': row[0],
                'title': row[1] or f'Dataset {row[0][:8]}...',
                'volatility': row[2] or 0.0
            }
            for row in cursor.fetchall()
        ]
        
        # Get at-risk datasets (datasets with high volatility and recent issues)
        cursor.execute('''
            SELECT 
                ds.dataset_id,
                ds.title,
                ds.snapshot_date,
                (ds.row_count * ds.column_count) / 1000000.0 as risk_score
            FROM dataset_states ds
            INNER JOIN (
                SELECT dataset_id, MAX(created_at) as max_created
                FROM dataset_states 
                GROUP BY dataset_id
            ) latest ON ds.dataset_id = latest.dataset_id 
            AND ds.created_at = latest.max_created
            WHERE ds.agency = ? 
            AND ds.availability = 'available'
            AND (ds.row_count * ds.column_count) / 1000000.0 > 0.5
            ORDER BY risk_score DESC
            LIMIT 5
        ''', (agency_name,))
        
        most_at_risk_datasets = [
            {
                'dataset_id': row[0],
                'title': row[1] or f'Dataset {row[0][:8]}...',
                'last_seen': row[2],
                'risk_score': row[3] or 0.0
            }
            for row in cursor.fetchall()
        ]
        
        conn.close()
        
        # Calculate additional metrics
        availability_rate = (available_datasets / total_datasets * 100) if total_datasets > 0 else 0
        license_stability = 0.8  # Placeholder - would need license data
        median_lifespan_days = 365  # Placeholder - would need historical data
        
        return jsonify({
            'agency_name': agency_name,
            'total_datasets': total_datasets or 0,
            'active_datasets': available_datasets or 0,
            'vanished_datasets': vanished_datasets or 0,
            'avg_volatility': avg_volatility,
            'license_stability': license_stability,
            'median_lifespan_days': median_lifespan_days,
            'monthly_trends': monthly_trends,
            'most_volatile_datasets': most_volatile_datasets,
            'most_at_risk_datasets': most_at_risk_datasets,
            'availability_rate': round(availability_rate, 1),
            'format_diversity': format_diversity or 0
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/analytics/trends')
def api_analytics_trends():
    """API endpoint for dataset trends data"""
    try:
        period = request.args.get('period', '30')
        source = request.args.get('source', 'all')
        
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Get trends data for the specified period
        cursor.execute('''
            SELECT 
                DATE(created_at) as date,
                COUNT(DISTINCT dataset_id) as total_datasets,
                COUNT(DISTINCT CASE WHEN availability = 'available' THEN dataset_id END) as available_datasets
            FROM dataset_states
            WHERE created_at >= date('now', '-{} days')
            GROUP BY DATE(created_at)
            ORDER BY date
        '''.format(period))
        
        trends_data = cursor.fetchall()
        
        labels = []
        total = []
        available = []
        
        for row in trends_data:
            labels.append(row[0])
            total.append(row[1])
            available.append(row[2])
        
        conn.close()
        
        return jsonify({
            'labels': labels,
            'total': total,
            'available': available,
            'stats': {
                'label': 'Total Datasets',
                'value': f'{sum(total):,}'
            }
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/analytics/status')
def api_analytics_status():
    """API endpoint for status distribution data"""
    try:
        source = request.args.get('source', 'all')
        
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Get status distribution
        cursor.execute('''
            SELECT 
                availability,
                COUNT(DISTINCT dataset_id) as count
            FROM dataset_states
            INNER JOIN (
                SELECT dataset_id, MAX(created_at) as max_created
                FROM dataset_states 
                GROUP BY dataset_id
            ) latest ON dataset_states.dataset_id = latest.dataset_id 
            AND dataset_states.created_at = latest.max_created
            GROUP BY availability
        ''')
        
        status_data = cursor.fetchall()
        
        labels = []
        values = []
        
        for row in status_data:
            labels.append(row[0].title() if row[0] else 'Unknown')
            values.append(row[1])
        
        conn.close()
        
        return jsonify({
            'labels': labels,
            'values': values,
            'stats': {
                'label': 'Available',
                'value': f'{values[0] if values else 0:,}'
            }
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/analytics/agencies')
def api_analytics_agencies():
    """API endpoint for agency performance data"""
    try:
        filter_type = request.args.get('filter', 'all')
        
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Get agency performance data
        limit = 20 if filter_type == 'top20' else 10 if filter_type == 'top10' else 50
        
        cursor.execute('''
            SELECT 
                agency,
                COUNT(DISTINCT dataset_id) as dataset_count
            FROM dataset_states
            INNER JOIN (
                SELECT dataset_id, MAX(created_at) as max_created
                FROM dataset_states 
                GROUP BY dataset_id
            ) latest ON dataset_states.dataset_id = latest.dataset_id 
            AND dataset_states.created_at = latest.max_created
            WHERE agency IS NOT NULL AND agency != ''
            GROUP BY agency
            ORDER BY dataset_count DESC
            LIMIT ?
        ''', (limit,))
        
        agency_data = cursor.fetchall()
        
        labels = []
        values = []
        
        for row in agency_data:
            labels.append(row[0][:20] + '...' if len(row[0]) > 20 else row[0])
            values.append(row[1])
        
        conn.close()
        
        return jsonify({
            'labels': labels,
            'values': values,
            'stats': {
                'label': 'Top Agency',
                'value': f'{labels[0] if labels else "N/A"} ({values[0] if values else 0:,})'
            }
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/analytics/changes')
def api_analytics_changes():
    """API endpoint for change frequency data"""
    try:
        period = request.args.get('period', '30')
        
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Get change frequency data
        cursor.execute('''
            SELECT 
                DATE(last_checked) as date,
                COUNT(*) as change_count
            FROM live_monitoring
            WHERE last_checked >= date('now', '-{} days')
            AND change_detected = 1
            GROUP BY DATE(last_checked)
            ORDER BY date
        '''.format(period))
        
        changes_data = cursor.fetchall()
        
        labels = []
        values = []
        
        for row in changes_data:
            labels.append(row[0])
            values.append(row[1])
        
        conn.close()
        
        return jsonify({
            'labels': labels,
            'values': values,
            'stats': {
                'label': 'Total Changes',
                'value': f'{sum(values):,}'
            }
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/analytics/quality')
def api_analytics_quality():
    """API endpoint for data quality metrics"""
    try:
        period = request.args.get('period', '30')
        
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Get quality metrics
        cursor.execute('''
            SELECT 
                AVG(CASE WHEN row_count > 0 AND column_count > 0 THEN 100 ELSE 0 END) as overall_quality,
                MAX(CASE WHEN row_count > 0 AND column_count > 0 THEN 100 ELSE 0 END) as best_quality,
                MIN(CASE WHEN row_count > 0 AND column_count > 0 THEN 100 ELSE 0 END) as worst_quality
            FROM dataset_states
            WHERE created_at >= date('now', '-{} days')
        '''.format(period))
        
        quality_data = cursor.fetchone()
        
        overall = quality_data[0] or 0
        best = quality_data[1] or 0
        worst = quality_data[2] or 0
        
        conn.close()
        
        return jsonify({
            'labels': ['Overall', 'Best', 'Worst'],
            'values': [round(overall, 1), round(best, 1), round(worst, 1)],
            'stats': {
                'label': 'Overall Score',
                'value': f'{round(overall, 1)}%'
            }
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/quality/summary')
def api_quality_summary():
    """API endpoint for quality summary data"""
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Get overall quality metrics
        cursor.execute('''
            SELECT 
                COUNT(DISTINCT dataset_id) as total_datasets,
                COUNT(DISTINCT CASE WHEN availability = 'available' THEN dataset_id END) as available_datasets,
                COUNT(DISTINCT CASE WHEN title IS NOT NULL AND title != '' AND agency IS NOT NULL AND agency != '' THEN dataset_id END) as complete_datasets
            FROM dataset_states
            INNER JOIN (
                SELECT dataset_id, MAX(created_at) as max_created
                FROM dataset_states 
                GROUP BY dataset_id
            ) latest ON dataset_states.dataset_id = latest.dataset_id 
            AND dataset_states.created_at = latest.max_created
        ''')
        
        summary_data = cursor.fetchone()
        total_datasets, available_datasets, complete_datasets = summary_data
        
        # Calculate rates
        availability_rate = round((available_datasets / total_datasets * 100) if total_datasets > 0 else 0, 1)
        completeness_rate = round((complete_datasets / total_datasets * 100) if total_datasets > 0 else 0, 1)
        
        # Calculate overall quality score (weighted average)
        overall_quality_score = round((availability_rate * 0.4 + completeness_rate * 0.6), 1)
        
        conn.close()
        
        return jsonify({
            'overall_quality_score': overall_quality_score,
            'total_datasets': total_datasets or 0,
            'available_datasets': available_datasets or 0,
            'complete_datasets': complete_datasets or 0,
            'availability_rate': availability_rate,
            'completeness_rate': completeness_rate
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/quality/metrics')
def api_quality_metrics():
    """API endpoint for quality metrics by agency"""
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Get quality metrics by agency
        cursor.execute('''
            SELECT 
                agency,
                COUNT(DISTINCT dataset_id) as total_datasets,
                COUNT(DISTINCT CASE WHEN availability = 'available' THEN dataset_id END) as available_datasets,
                COUNT(DISTINCT CASE WHEN title IS NOT NULL AND title != '' AND agency IS NOT NULL AND agency != '' THEN dataset_id END) as complete_datasets
            FROM dataset_states
            INNER JOIN (
                SELECT dataset_id, MAX(created_at) as max_created
                FROM dataset_states 
                GROUP BY dataset_id
            ) latest ON dataset_states.dataset_id = latest.dataset_id 
            AND dataset_states.created_at = latest.max_created
            WHERE agency IS NOT NULL AND agency != ''
            GROUP BY agency
            HAVING total_datasets >= 5
            ORDER BY total_datasets DESC
            LIMIT 20
        ''')
        
        agency_metrics = []
        for row in cursor.fetchall():
            agency, total, available, complete = row
            availability_rate = round((available / total * 100) if total > 0 else 0, 1)
            completeness_rate = round((complete / total * 100) if total > 0 else 0, 1)
            quality_score = round((availability_rate * 0.4 + completeness_rate * 0.6), 1)
            
            agency_metrics.append({
                'agency': agency,
                'total_datasets': total,
                'available_datasets': available,
                'complete_datasets': complete,
                'availability_rate': availability_rate,
                'completeness_rate': completeness_rate,
                'quality_score': quality_score
            })
        
        conn.close()
        
        return jsonify({
            'agency_metrics': agency_metrics
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/quality/issues')
def api_quality_issues():
    """API endpoint for quality issues"""
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Get datasets with quality issues
        cursor.execute('''
            SELECT 
                agency,
                COUNT(DISTINCT dataset_id) as total_datasets,
                COUNT(DISTINCT CASE WHEN availability = 'unavailable' THEN dataset_id END) as unavailable_datasets,
                COUNT(DISTINCT CASE WHEN title IS NULL OR title = '' THEN dataset_id END) as missing_title,
                COUNT(DISTINCT CASE WHEN agency IS NULL OR agency = '' THEN dataset_id END) as missing_agency,
                COUNT(DISTINCT CASE WHEN url IS NULL OR url = '' THEN dataset_id END) as missing_url
            FROM dataset_states
            INNER JOIN (
                SELECT dataset_id, MAX(created_at) as max_created
                FROM dataset_states 
                GROUP BY dataset_id
            ) latest ON dataset_states.dataset_id = latest.dataset_id 
            AND dataset_states.created_at = latest.max_created
            WHERE agency IS NOT NULL AND agency != ''
            GROUP BY agency
            HAVING total_datasets >= 5
            ORDER BY unavailable_datasets DESC, missing_title DESC
            LIMIT 15
        ''')
        
        quality_issues = []
        for row in cursor.fetchall():
            agency, total, unavailable, missing_title, missing_agency, missing_url = row
            
            issues = []
            severity = 'low'
            
            if unavailable > 0:
                issues.append(f'{unavailable} unavailable datasets')
                if unavailable / total > 0.2:
                    severity = 'high'
                elif unavailable / total > 0.1:
                    severity = 'medium'
            
            if missing_title > 0:
                issues.append(f'{missing_title} missing titles')
                if missing_title / total > 0.3:
                    severity = 'high' if severity == 'low' else severity
                elif missing_title / total > 0.1:
                    severity = 'medium' if severity == 'low' else severity
            
            if missing_agency > 0:
                issues.append(f'{missing_agency} missing agencies')
                severity = 'high' if severity == 'low' else severity
            
            if missing_url > 0:
                issues.append(f'{missing_url} missing URLs')
                if missing_url / total > 0.5:
                    severity = 'high' if severity == 'low' else severity
                elif missing_url / total > 0.2:
                    severity = 'medium' if severity == 'low' else severity
            
            if issues:
                quality_issues.append({
                    'title': f'Data Quality Issues in {agency}',
                    'agency': agency,
                    'severity': severity,
                    'issues': issues,
                    'total_datasets': total,
                    'issue_count': len(issues)
                })
        
        conn.close()
        
        return jsonify({
            'quality_issues': quality_issues
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/quality/recommendations')
def api_quality_recommendations():
    """API endpoint for improvement recommendations"""
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Get data for recommendations
        cursor.execute('''
            SELECT 
                COUNT(DISTINCT dataset_id) as total_datasets,
                COUNT(DISTINCT CASE WHEN availability = 'unavailable' THEN dataset_id END) as unavailable_datasets,
                COUNT(DISTINCT CASE WHEN title IS NULL OR title = '' THEN dataset_id END) as missing_title,
                COUNT(DISTINCT CASE WHEN agency IS NULL OR agency = '' THEN dataset_id END) as missing_agency,
                COUNT(DISTINCT CASE WHEN url IS NULL OR url = '' THEN dataset_id END) as missing_url,
                COUNT(DISTINCT CASE WHEN row_count IS NULL OR row_count = 0 THEN dataset_id END) as missing_row_count,
                COUNT(DISTINCT CASE WHEN column_count IS NULL OR column_count = 0 THEN dataset_id END) as missing_column_count
            FROM dataset_states
            INNER JOIN (
                SELECT dataset_id, MAX(created_at) as max_created
                FROM dataset_states 
                GROUP BY dataset_id
            ) latest ON dataset_states.dataset_id = latest.dataset_id 
            AND dataset_states.created_at = latest.max_created
        ''')
        
        data = cursor.fetchone()
        total, unavailable, missing_title, missing_agency, missing_url, missing_rows, missing_cols = data
        
        recommendations = []
        
        # Availability recommendations
        if unavailable > 0:
            unavailable_rate = unavailable / total
            if unavailable_rate > 0.1:
                recommendations.append({
                    'category': 'Dataset Availability',
                    'priority': 'high' if unavailable_rate > 0.2 else 'medium',
                    'description': f'{unavailable} datasets ({unavailable_rate:.1%}) are currently unavailable',
                    'action': 'Investigate and restore access to unavailable datasets',
                    'impact': 'Improves data accessibility and user experience'
                })
        
        # Metadata completeness recommendations
        if missing_title > 0:
            title_rate = missing_title / total
            recommendations.append({
                'category': 'Metadata Completeness',
                'priority': 'high' if title_rate > 0.3 else 'medium',
                'description': f'{missing_title} datasets ({title_rate:.1%}) are missing titles',
                'action': 'Add descriptive titles to all datasets',
                'impact': 'Improves dataset discoverability and usability'
            })
        
        if missing_agency > 0:
            agency_rate = missing_agency / total
            recommendations.append({
                'category': 'Metadata Completeness',
                'priority': 'high' if agency_rate > 0.2 else 'medium',
                'description': f'{missing_agency} datasets ({agency_rate:.1%}) are missing agency information',
                'action': 'Assign proper agency attribution to all datasets',
                'impact': 'Enables proper data governance and accountability'
            })
        
        if missing_url > 0:
            url_rate = missing_url / total
            recommendations.append({
                'category': 'Data Access',
                'priority': 'high' if url_rate > 0.5 else 'medium',
                'description': f'{missing_url} datasets ({url_rate:.1%}) are missing URLs',
                'action': 'Add direct access URLs to all datasets',
                'impact': 'Enables direct data access and download'
            })
        
        # Data structure recommendations
        if missing_rows > 0 or missing_cols > 0:
            structure_rate = (missing_rows + missing_cols) / (total * 2)
            recommendations.append({
                'category': 'Data Structure',
                'priority': 'medium' if structure_rate > 0.3 else 'low',
                'description': f'{missing_rows + missing_cols} datasets missing row/column count information',
                'action': 'Add data structure metadata to improve data understanding',
                'impact': 'Helps users understand dataset size and complexity'
            })
        
        # General recommendations
        recommendations.extend([
            {
                'category': 'Data Governance',
                'priority': 'medium',
                'description': 'Implement regular data quality monitoring',
                'action': 'Set up automated quality checks and alerts',
                'impact': 'Proactive identification and resolution of quality issues'
            },
            {
                'category': 'User Experience',
                'priority': 'low',
                'description': 'Improve dataset search and filtering capabilities',
                'action': 'Enhance search functionality with better metadata indexing',
                'impact': 'Makes datasets easier to find and use'
            }
        ])
        
        conn.close()
        
        return jsonify({
            'recommendations': recommendations
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/quality/export')
def api_quality_export():
    """API endpoint for exporting quality reports"""
    try:
        format_type = request.args.get('format', 'json')
        
        # Get all quality data
        summary_response = api_quality_summary()
        metrics_response = api_quality_metrics()
        issues_response = api_quality_issues()
        recommendations_response = api_quality_recommendations()
        
        summary_data = summary_response.get_json()
        metrics_data = metrics_response.get_json()
        issues_data = issues_response.get_json()
        recommendations_data = recommendations_response.get_json()
        
        export_data = {
            'exported_at': datetime.now().isoformat(),
            'summary': summary_data,
            'agency_metrics': metrics_data.get('agency_metrics', []),
            'quality_issues': issues_data.get('quality_issues', []),
            'recommendations': recommendations_data.get('recommendations', [])
        }
        
        if format_type == 'json':
            return jsonify(export_data)
        elif format_type == 'csv':
            # Convert to CSV format
            import io
            import csv
            
            output = io.StringIO()
            writer = csv.writer(output)
            
            # Write summary
            writer.writerow(['Metric', 'Value'])
            writer.writerow(['Overall Quality Score', summary_data.get('overall_quality_score', 0)])
            writer.writerow(['Total Datasets', summary_data.get('total_datasets', 0)])
            writer.writerow(['Available Datasets', summary_data.get('available_datasets', 0)])
            writer.writerow(['Availability Rate', summary_data.get('availability_rate', 0)])
            writer.writerow(['Completeness Rate', summary_data.get('completeness_rate', 0)])
            writer.writerow([])
            
            # Write agency metrics
            writer.writerow(['Agency', 'Total Datasets', 'Available Datasets', 'Quality Score'])
            for metric in metrics_data.get('agency_metrics', []):
                writer.writerow([
                    metric.get('agency', ''),
                    metric.get('total_datasets', 0),
                    metric.get('available_datasets', 0),
                    metric.get('quality_score', 0)
                ])
            
            response = make_response(output.getvalue())
            response.headers['Content-Type'] = 'text/csv'
            response.headers['Content-Disposition'] = 'attachment; filename=quality_report.csv'
            return response
        
        elif format_type == 'pdf':
            # For PDF, return JSON for now (would need PDF generation library)
            return jsonify({
                'message': 'PDF export not yet implemented',
                'data': export_data
            })
        
        return jsonify(export_data)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/search')
def api_search():
    """API endpoint for searching datasets"""
    try:
        query = request.args.get('q', '').lower()
        datasets_response = api_datasets()
        datasets = datasets_response.get_json()
        
        if query:
            filtered = []
            for dataset in datasets:
                if (query in dataset.get('title', '').lower() or 
                    query in dataset.get('agency', '').lower() or
                    query in dataset.get('dataset_id', '').lower()):
                    filtered.append(dataset)
            return jsonify(filtered)
        
        return jsonify(datasets)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ============================================================================
# ENHANCED API ENDPOINTS - Complete Dataset State Historian Integration
# ============================================================================

@app.route('/api/lil/snapshots')
def api_lil_snapshots():
    """Get available LIL snapshots"""
    try:
        snapshots = lil_integration.get_available_snapshots()
        return jsonify({
            'snapshots': snapshots,
            'count': len(snapshots)
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/lil/snapshot/<snapshot_date>')
def api_lil_snapshot(snapshot_date):
    """Get datasets for specific LIL snapshot"""
    try:
        datasets = lil_integration.get_snapshot_datasets(snapshot_date)
        return jsonify({
            'snapshot_date': snapshot_date,
            'datasets': datasets,
            'count': len(datasets)
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/lil/compare')
def api_lil_compare():
    """Compare LIL snapshots with live catalog"""
    try:
        # Get live datasets
        live_datasets = []
        conn = get_database_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT ds.dataset_id, ds.title, ds.agency, ds.url, ds.availability
            FROM dataset_states ds
            INNER JOIN (
                SELECT dataset_id, MAX(created_at) as max_created
                FROM dataset_states 
                GROUP BY dataset_id
            ) latest ON ds.dataset_id = latest.dataset_id 
            AND ds.created_at = latest.max_created
        ''')
        
        columns = [description[0] for description in cursor.description]
        live_datasets = [dict(zip(columns, row)) for row in cursor.fetchall()]
        conn.close()
        
        # Compare with LIL
        comparison = lil_integration.compare_with_live_catalog(live_datasets)
        return jsonify(comparison)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/lil/refresh', methods=['POST'])
def api_lil_refresh():
    """Refresh live dataset count from Data.gov API"""
    try:
        result = lil_integration.refresh_live_dataset_count()
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/volatility/<dataset_id>')
def api_volatility(dataset_id):
    """Get volatility metrics for dataset"""
    try:
        metrics = enhanced_diff_engine.get_volatility_metrics(dataset_id)
        return jsonify({
            'dataset_id': dataset_id,
            'metrics': metrics
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/events/<dataset_id>')
def api_events(dataset_id):
    """Get change events for dataset"""
    try:
        events = enhanced_diff_engine.get_change_events(dataset_id)
        return jsonify({
            'dataset_id': dataset_id,
            'events': events
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/chromogram/<dataset_id>')
def api_chromogram(dataset_id):
    """Get Chromogram timeline data for dataset"""
    try:
        days = request.args.get('days', 30, type=int)
        data = chromogram_timeline.generate_chromogram_data(dataset_id, days)
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/chromogram/<dataset_id>/html')
def api_chromogram_html(dataset_id):
    """Generate Chromogram HTML for dataset"""
    try:
        days = request.args.get('days', 30, type=int)
        html_path = chromogram_timeline.save_chromogram_html(
            dataset_id, days, 
            f"chromogram_{dataset_id}_{days}days.html"
        )
        return jsonify({
            'dataset_id': dataset_id,
            'html_path': html_path,
            'url': f"/static/{html_path}"
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/postmortem/<dataset_id>')
def api_postmortem(dataset_id):
    """Generate post-mortem analysis for vanished dataset"""
    try:
        from analysis.postmortem_system import PostMortemSystem
        
        # Initialize post-mortem system
        postmortem_system = PostMortemSystem()
        
        # Generate comprehensive post-mortem analysis
        analysis = postmortem_system.generate_postmortem(dataset_id)
        
        return jsonify(analysis)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ============================================================================
# FULL DATABASE PROCESSING ENDPOINTS
# ============================================================================

@app.route('/api/full-database/status')
def api_full_database_status():
    """Get full database processing status"""
    try:
        stats = full_database_processor.get_processing_stats()
        return jsonify({
            'status': 'success',
            'stats': stats,
            'is_processing': full_processing_thread and full_processing_thread.is_alive()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/full-database/start', methods=['POST'])
def api_start_full_database_processing():
    """Start full database processing"""
    global full_processing_thread
    
    try:
        if full_processing_thread and full_processing_thread.is_alive():
            return jsonify({'status': 'already_running'})
        
        max_datasets = request.json.get('max_datasets') if request.json else None
        
        def run_processing():
            asyncio.run(full_database_processor.process_full_database(max_datasets=max_datasets))
        
        full_processing_thread = threading.Thread(target=run_processing, daemon=True)
        full_processing_thread.start()
        
        return jsonify({
            'status': 'started',
            'max_datasets': max_datasets,
            'message': 'Full database processing started'
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/full-database/stop', methods=['POST'])
def api_stop_full_database_processing():
    """Stop full database processing"""
    global full_processing_thread
    
    try:
        if full_processing_thread and full_processing_thread.is_alive():
            # Note: This is a graceful stop - the thread will finish its current batch
            return jsonify({'status': 'stopping', 'message': 'Processing will stop after current batch'})
        else:
            return jsonify({'status': 'not_running', 'message': 'No processing in progress'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/scaled-monitor/status')
def api_scaled_monitor_status():
    """Get scaled monitoring status"""
    try:
        stats = scaled_monitor.get_monitoring_stats()
        return jsonify({
            'status': 'success',
            'stats': stats
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/scaled-monitor/start', methods=['POST'])
def api_start_scaled_monitoring():
    """Start scaled monitoring"""
    try:
        sample_size = request.json.get('sample_size') if request.json else None
        
        def run_monitoring():
            asyncio.run(scaled_monitor.start_monitoring(sample_size=sample_size))
        
        monitoring_thread = threading.Thread(target=run_monitoring, daemon=True)
        monitoring_thread.start()
        
        return jsonify({
            'status': 'started',
            'sample_size': sample_size,
            'message': 'Scaled monitoring started'
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/scaled-monitor/stop', methods=['POST'])
def api_stop_scaled_monitoring():
    """Stop scaled monitoring"""
    try:
        scaled_monitor.stop_monitoring()
        return jsonify({
            'status': 'stopped',
            'message': 'Scaled monitoring stopped'
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/trigger_monitoring', methods=['POST'])
def api_trigger_monitoring():
    """Trigger a fresh monitoring cycle"""
    try:
        # Run a quick monitoring cycle to get fresh data
        def run_quick_check():
            asyncio.run(monitor.run_quick_check())
        
        # Run in background thread
        threading.Thread(target=run_quick_check, daemon=True).start()
        
        return jsonify({
            'status': 'triggered',
            'message': 'Fresh monitoring cycle triggered'
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/dataset/<dataset_id>/column-changes')
def api_column_changes(dataset_id):
    """API endpoint for column-level changes"""
    try:
        from enhanced_column_diffing import EnhancedColumnDiffing
        
        diffing = EnhancedColumnDiffing()
        changes = diffing.get_column_changes(dataset_id)
        
        return jsonify({
            'dataset_id': dataset_id,
            'changes': changes,
            'count': len(changes)
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/dataset/<dataset_id>/schema-evolution')
def api_schema_evolution(dataset_id):
    """API endpoint for schema evolution over time"""
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Get schema snapshots
        cursor.execute('''
            SELECT snapshot_date, columns, column_types, row_count, sample_data
            FROM schema_snapshots
            WHERE dataset_id = ?
            ORDER BY snapshot_date ASC
        ''', (dataset_id,))
        
        evolution = []
        for row in cursor.fetchall():
            evolution.append({
                'date': row[0],
                'columns': json.loads(row[1]) if row[1] else [],
                'column_types': json.loads(row[2]) if row[2] else {},
                'row_count': row[3],
                'sample_data': json.loads(row[4]) if row[4] else []
            })
        
        conn.close()
        
        return jsonify({
            'dataset_id': dataset_id,
            'evolution': evolution,
            'snapshots': len(evolution)
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ============================================================================
# VANISHED DATASETS COMPARISON ENDPOINTS
# ============================================================================

@app.route('/api/vanished-datasets')
def api_vanished_datasets():
    """API endpoint for vanished datasets (in LIL but not in live Data.gov)"""
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Get vanished datasets from the vanished_datasets table
        cursor.execute('''
            SELECT vd.dataset_id, vd.last_known_title as title, vd.last_known_agency as agency, 
                   vd.last_known_landing_page as original_url, 
                   CASE 
                       WHEN vd.last_seen_date IS NOT NULL AND vd.last_seen_date != '' 
                       THEN vd.last_seen_date 
                       ELSE vd.disappearance_date 
                   END as last_seen_date, 
                   vd.archival_sources as archive_url, vd.status, vd.created_at
            FROM vanished_datasets vd
            ORDER BY vd.disappearance_date DESC, vd.last_seen_date DESC
            LIMIT 1000
        ''')
        
        columns = [description[0] for description in cursor.description]
        vanished = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        conn.close()
        
        return jsonify({
            'vanished_datasets': vanished,
            'count': len(vanished),
            'last_updated': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/vanished-datasets/stats')
def api_vanished_datasets_stats():
    """API endpoint for vanished datasets statistics"""
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Total vanished datasets
        cursor.execute('SELECT COUNT(*) FROM vanished_datasets')
        total_vanished = cursor.fetchone()[0] or 0
        
        # By status
        cursor.execute('''
            SELECT status, COUNT(*) as count
            FROM vanished_datasets
            GROUP BY status
        ''')
        status_breakdown = dict(cursor.fetchall())
        
        # By suspected cause
        cursor.execute('''
            SELECT suspected_cause, COUNT(*) as count
            FROM vanished_datasets
            GROUP BY suspected_cause
        ''')
        cause_breakdown = dict(cursor.fetchall())
        
        # By agency
        cursor.execute('''
            SELECT agency, COUNT(*) as count
            FROM vanished_datasets
            WHERE agency IS NOT NULL
            GROUP BY agency
            ORDER BY count DESC
            LIMIT 10
        ''')
        agency_breakdown = [{'agency': row[0], 'count': row[1]} for row in cursor.fetchall()]
        
        # Recent vanishings (last 30 days)
        cursor.execute('''
            SELECT COUNT(*) FROM vanished_datasets
            WHERE last_seen_date >= date('now', '-30 days')
        ''')
        recent_vanished = cursor.fetchone()[0] or 0
        
        # Get LIL vs Data.gov comparison stats
        cursor.execute('SELECT COUNT(DISTINCT dataset_id) FROM lil_manifests')
        lil_datasets = cursor.fetchone()[0] or 0
        
        cursor.execute('SELECT COUNT(DISTINCT dataset_id) FROM dataset_states')
        live_datasets = cursor.fetchone()[0] or 0
        
        conn.close()
        
        return jsonify({
            'total_vanished': total_vanished,
            'status_breakdown': status_breakdown,
            'cause_breakdown': cause_breakdown,
            'agency_breakdown': agency_breakdown,
            'recent_vanished_30d': recent_vanished,
            'comparison': {
                'lil_datasets': lil_datasets,
                'live_datasets': live_datasets,
                'vanished_count': total_vanished,
                'retention_rate': round((live_datasets / (lil_datasets + live_datasets)) * 100, 2) if (lil_datasets + live_datasets) > 0 else 0
            },
            'last_updated': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/vanished-datasets/compare', methods=['POST'])
def api_compare_datasets():
    """API endpoint to run LIL vs Data.gov comparison"""
    try:
        # Run the comparison in a background thread
        def run_comparison():
            try:
                # Import the comparison logic
                from core.data_fetcher import DataFetcher
                from analysis.diff_engine import DiffEngine
                
                # Initialize components
                data_fetcher = DataFetcher()
                diff_engine = DiffEngine()
                
                # Fetch LIL manifest (archived datasets)
                print("Fetching LIL manifest...")
                lil_data = data_fetcher.fetch_lil_manifest()
                print(f"Found {len(lil_data)} datasets in LIL archive")
                
                # Fetch live Data.gov catalog
                print("Fetching live Data.gov catalog...")
                live_data = data_fetcher.fetch_live_datagov_catalog()
                print(f"Found {len(live_data)} datasets in live catalog")
                
                # Find vanished datasets
                print("Comparing datasets...")
                vanished = diff_engine.find_vanished_datasets()
                print(f"Found {len(vanished)} vanished datasets")
                
                # Store vanished datasets in database
                if vanished:
                    store_vanished_datasets(vanished)
                    print(f"Stored {len(vanished)} vanished datasets in database")
                
            except Exception as e:
                print(f"Error in comparison process: {e}")
        
        # Start comparison in background thread
        import threading
        comparison_thread = threading.Thread(target=run_comparison, daemon=True)
        comparison_thread.start()
        
        return jsonify({
            'status': 'comparison_started',
            'message': 'Dataset comparison process initiated - checking LIL vs Data.gov',
            'estimated_time': '2-5 minutes'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def store_vanished_datasets(vanished_datasets):
    """Store vanished datasets in the database"""
    conn = get_database_connection()
    cursor = conn.cursor()
    
    # Create vanished_datasets table if it doesn't exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS vanished_datasets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dataset_id TEXT UNIQUE,
            title TEXT,
            agency TEXT,
            original_url TEXT,
            last_seen_date TEXT,
            suspected_cause TEXT,
            status TEXT,
            archive_url TEXT,
            wayback_url TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    for dataset in vanished_datasets:
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO vanished_datasets
                (dataset_id, title, agency, original_url, last_seen_date, 
                 suspected_cause, status, archive_url, wayback_url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                dataset.get('id', ''),
                dataset.get('title', ''),
                dataset.get('organization', {}).get('title', '') if isinstance(dataset.get('organization'), dict) else dataset.get('organization', ''),
                dataset.get('url', ''),
                dataset.get('last_seen', ''),
                dataset.get('suspected_cause', 'URL broken or catalog removal'),
                dataset.get('status', 'removed'),
                dataset.get('archive_url', ''),
                dataset.get('wayback_url', '')
            ))
        except Exception as e:
            print(f"Error storing vanished dataset {dataset.get('id', 'unknown')}: {e}")
            continue
    
    conn.commit()
    conn.close()

@app.route('/api/vanished-datasets/<dataset_id>')
def api_vanished_dataset_detail(dataset_id):
    """API endpoint for specific vanished dataset details"""
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Get vanished dataset details
        cursor.execute('''
            SELECT vd.*, wb.last_captured, wb.wayback_url
            FROM vanished_datasets vd
            LEFT JOIN wayback_availability wb ON vd.dataset_id = wb.dataset_id
            WHERE vd.dataset_id = ?
        ''', (dataset_id,))
        
        result = cursor.fetchone()
        if not result:
            return jsonify({'error': 'Vanished dataset not found'}), 404
        
        columns = [description[0] for description in cursor.description]
        dataset = dict(zip(columns, result))
        
        # Get historical timeline
        cursor.execute('''
            SELECT snapshot_date, title, agency, availability, created_at
            FROM dataset_states
            WHERE dataset_id = ?
            ORDER BY created_at ASC
        ''', (dataset_id,))
        
        timeline = []
        for row in cursor.fetchall():
            timeline.append({
                'date': row[0],
                'title': row[1],
                'agency': row[2],
                'availability': row[3],
                'timestamp': row[4]
            })
        
        conn.close()
        
        return jsonify({
            'dataset': dataset,
            'timeline': timeline,
            'last_updated': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ============================================================================
# DIMENSION COMPUTATION ENDPOINTS
# ============================================================================

@app.route('/api/dimensions/statistics')
def api_dimensions_statistics():
    """Get comprehensive dimension statistics"""
    try:
        stats = dimension_computer.get_dimension_statistics()
        return jsonify({
            'status': 'success',
            'statistics': stats
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/dimensions/backfill', methods=['POST'])
def api_dimensions_backfill():
    """Start backfill process for missing dimensions"""
    try:
        force_recompute = request.json.get('force_recompute', False) if request.json else False
        
        def run_backfill():
            return asyncio.run(dimension_backfill.backfill_all_missing_dimensions(force_recompute))
        
        # Run in background thread
        backfill_thread = threading.Thread(target=run_backfill, daemon=True)
        backfill_thread.start()
        
        return jsonify({
            'status': 'started',
            'force_recompute': force_recompute,
            'message': 'Dimension backfill process started'
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/dimensions/compute', methods=['POST'])
def api_dimensions_compute():
    """Start comprehensive dimension computation"""
    try:
        force_recompute = request.json.get('force_recompute', False) if request.json else False
        
        def run_computation():
            return asyncio.run(dimension_computer.ensure_all_datasets_have_dimensions(force_recompute))
        
        # Run in background thread
        computation_thread = threading.Thread(target=run_computation, daemon=True)
        computation_thread.start()
        
        return jsonify({
            'status': 'started',
            'force_recompute': force_recompute,
            'message': 'Dimension computation process started'
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/dimensions/validate')
def api_dimensions_validate():
    """Validate dimension data quality"""
    try:
        validation = dimension_backfill.validate_dimension_data()
        return jsonify({
            'status': 'success',
            'validation': validation
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/dimensions/backfill/status')
def api_dimensions_backfill_status():
    """Get backfill process status and statistics"""
    try:
        stats = dimension_backfill.get_backfill_statistics()
        return jsonify({
            'status': 'success',
            'statistics': stats
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/dimensions/missing')
def api_dimensions_missing():
    """Get list of datasets missing dimension data"""
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT ds.dataset_id, ds.title, ds.agency, ds.url, ds.resource_format, ds.created_at
            FROM dataset_states ds
            INNER JOIN (
                SELECT dataset_id, MAX(created_at) as max_created
                FROM dataset_states 
                GROUP BY dataset_id
            ) latest ON ds.dataset_id = latest.dataset_id 
            AND ds.created_at = latest.max_created
            WHERE (ds.row_count IS NULL OR ds.row_count = 0 OR ds.column_count IS NULL OR ds.column_count = 0)
            AND ds.availability = 'available'
            ORDER BY ds.created_at DESC
            LIMIT 100
        ''')
        
        columns = [description[0] for description in cursor.description]
        missing_datasets = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        conn.close()
        
        return jsonify({
            'status': 'success',
            'missing_datasets': missing_datasets,
            'count': len(missing_datasets)
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500



@app.route('/api/volatility/stats')
def api_volatility_stats():
    """Get volatility statistics"""
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Get volatility metrics
        cursor.execute('''
            SELECT 
                COUNT(*) as total_datasets,
                AVG(volatility_score) as avg_volatility,
                MAX(volatility_score) as max_volatility,
                COUNT(CASE WHEN volatility_score > 0.7 THEN 1 END) as high_volatility_count
            FROM volatility_metrics
        ''')
        
        stats = cursor.fetchone()
        
        conn.close()
        
        return jsonify({
            'total_datasets': stats[0] or 0,
            'avg_volatility': round(stats[1] or 0, 3),
            'max_volatility': round(stats[2] or 0, 3),
            'high_volatility_count': stats[3] or 0,
            'last_updated': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/volatility/ranking')
def api_volatility_ranking():
    """Get volatility ranking of datasets"""
    try:
        limit = request.args.get('limit', 20, type=int)

        conn = get_database_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT vm.dataset_id, vm.volatility_score, vm.churn_rate,
                   ds.title, ds.agency, ds.url
            FROM volatility_metrics vm
            LEFT JOIN dataset_states ds ON vm.dataset_id = ds.dataset_id
            ORDER BY vm.volatility_score DESC
            LIMIT ?
        ''', (limit,))

        columns = [description[0] for description in cursor.description]
        ranking = [dict(zip(columns, row)) for row in cursor.fetchall()]

        conn.close()

        return jsonify({
            'ranking': ranking,
            'count': len(ranking),
            'last_updated': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Enhanced API endpoints for historical analysis

@app.route('/api/availability/events')
def api_availability_events():
    """Get availability events (NEW, CHANGED, VANISHED)"""
    try:
        dataset_id = request.args.get('dataset_id')
        status = request.args.get('status')
        severity = request.args.get('severity')
        limit = request.args.get('limit', 100, type=int)
        
        detector = AvailabilityDetector()
        events = detector.get_availability_events(
            dataset_id=dataset_id,
            status=DatasetStatus(status) if status else None,
            severity=severity,
            limit=limit
        )
        
        return jsonify({
            'events': events,
            'count': len(events),
            'last_updated': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/diff/<dataset_id>/<from_date>/<to_date>')
def api_dataset_diff(dataset_id, from_date, to_date):
    """Get detailed diff between two snapshots"""
    try:
        diff_engine = EnhancedDiffEngineV2()
        diff_data = diff_engine.get_diff(dataset_id, from_date, to_date)
        
        if not diff_data:
            # Compute diff if not stored
            diff_result = diff_engine.compute_diff(dataset_id, from_date, to_date)
            if diff_result:
                diff_id = diff_engine.store_diff(diff_result)
                diff_data = diff_engine.get_diff(dataset_id, from_date, to_date)
        
        if not diff_data:
            return jsonify({'error': 'Diff not found'}), 404
        
        return jsonify(diff_data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/events/normalized')
def api_events_normalized():
    """Get normalized events with filtering"""
    try:
        dataset_id = request.args.get('dataset_id')
        event_type = request.args.get('event_type')
        severity = request.args.get('severity')
        date_from = request.args.get('date_from')
        date_to = request.args.get('date_to')
        limit = request.args.get('limit', 100, type=int)
        
        extractor = EventExtractor()
        events = extractor.get_events(
            dataset_id=dataset_id,
            event_type=EventType(event_type) if event_type else None,
            severity=EventSeverity(severity) if severity else None,
            date_from=date_from,
            date_to=date_to,
            limit=limit
        )
        
        return jsonify({
            'events': events,
            'count': len(events),
            'last_updated': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/timeline/chromogram/<dataset_id>')
def api_timeline_chromogram(dataset_id):
    """Get Chromogram timeline data for a dataset"""
    try:
        date_from = request.args.get('date_from')
        date_to = request.args.get('date_to')
        format = request.args.get('format', 'json')
        
        timeline = ChromogramTimelineV2()
        chromogram_data = timeline.generate_chromogram_data(
            dataset_id, date_from, date_to
        )
        
        if format == 'json':
            return jsonify({
                'dataset_id': chromogram_data.dataset_id,
                'date_range': chromogram_data.date_range,
                'bands': [
                    {
                        'name': band.name,
                        'type': band.band_type.value,
                        'fields': band.fields,
                        'color_map': band.color_map
                    }
                    for band in chromogram_data.bands
                ],
                'cells': [
                    {
                        'field': cell.field,
                        'date': cell.date,
                        'value': cell.value,
                        'changed': cell.changed,
                        'old_value': cell.old_value,
                        'color': cell.color
                    }
                    for cell in chromogram_data.cells
                ],
                'events': [
                    {
                        'date': event.date,
                        'event_type': event.event_type,
                        'severity': event.severity,
                        'description': event.description
                    }
                    for event in chromogram_data.events
                ],
                'vanished_date': chromogram_data.vanished_date
            })
        else:
            # Export in requested format
            exported_data = timeline.export_timeline_data(dataset_id, format)
            return exported_data, 200, {'Content-Type': 'text/plain'}
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/field-history/<dataset_id>/<field>')
def api_field_history(dataset_id, field):
    """Get change history for a specific field"""
    try:
        timeline = ChromogramTimelineV2()
        history = timeline.get_field_diff_history(dataset_id, field)
        
        return jsonify({
            'dataset_id': dataset_id,
            'field': field,
            'history': history,
            'count': len(history)
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/timeline-summary/<dataset_id>')
def api_timeline_summary(dataset_id):
    """Get timeline summary statistics"""
    try:
        timeline = ChromogramTimelineV2()
        summary = timeline.get_timeline_summary(dataset_id)
        
        return jsonify(summary)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/events/summary')
def api_events_summary():
    """Get event summary statistics"""
    try:
        extractor = EventExtractor()
        summary = extractor.get_event_summary()
        
        return jsonify(summary)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/process-availability')
def api_process_availability():
    """Process all snapshots to detect availability changes"""
    try:
        detector = AvailabilityDetector()
        events = detector.process_all_snapshots()
        
        # Extract events from availability changes
        extractor = EventExtractor()
        availability_events = detector.get_availability_events(limit=1000)
        normalized_events = extractor.extract_events_from_availability(availability_events)
        extractor.store_events(normalized_events)
        
        return jsonify({
            'message': 'Availability processing completed',
            'availability_events': len(events),
            'normalized_events': len(normalized_events),
            'last_updated': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/dimensions/agency-stats')
def api_dimensions_agency_stats():
    """Get dimension statistics by agency"""
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT ds.agency, 
                   COUNT(DISTINCT ds.dataset_id) as total_datasets,
                   COUNT(CASE WHEN ds.row_count > 0 AND ds.column_count > 0 THEN 1 END) as with_dimensions,
                   AVG(CASE WHEN ds.row_count > 0 THEN ds.row_count END) as avg_rows,
                   AVG(CASE WHEN ds.column_count > 0 THEN ds.column_count END) as avg_columns
            FROM dataset_states ds
            INNER JOIN (
                SELECT dataset_id, MAX(created_at) as max_created
                FROM dataset_states 
                GROUP BY dataset_id
            ) latest ON ds.dataset_id = latest.dataset_id 
            AND ds.created_at = latest.max_created
            WHERE ds.availability = 'available'
            GROUP BY ds.agency
            ORDER BY total_datasets DESC
            LIMIT 50
        ''')
        
        agency_stats = []
        for row in cursor.fetchall():
            agency, total, with_dimensions, avg_rows, avg_columns = row
            completion_rate = (with_dimensions / total * 100) if total > 0 else 0
            
            agency_stats.append({
                'agency': agency or 'Unknown',
                'total_datasets': total,
                'with_dimensions': with_dimensions,
                'completion_rate': round(completion_rate, 2),
                'average_rows': round(avg_rows or 0, 2),
                'average_columns': round(avg_columns or 0, 2)
            })
        
        conn.close()
        
        return jsonify({
            'status': 'success',
            'agency_statistics': agency_stats
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    # Create templates directory if it doesn't exist
    os.makedirs('templates', exist_ok=True)
    
    # Start monitoring automatically
    def start_auto_monitoring():
        try:
            def run_monitoring():
                import asyncio
                asyncio.run(monitor.start_continuous_monitoring(30))  # 30 minute intervals
            
            global monitoring_thread
            monitoring_thread = threading.Thread(target=run_monitoring, daemon=True)
            monitoring_thread.start()
            print("Auto-starting monitoring system...")
        except Exception as e:
            print(f"Failed to start monitoring: {e}")
    
# Duplicate route removed - using the first definition above

@app.route('/api/system/health')
def api_system_health():
    """Get system health check"""
    try:
        from monitoring.system_monitor import SystemMonitor
        
        monitor = SystemMonitor()
        health = asyncio.run(monitor._check_system_health())
        
        return jsonify(health)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/system/alerts')
def api_system_alerts():
    """Get system alerts"""
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Get recent alerts
        cursor.execute('''
            SELECT alert_type, severity, message, details, created_at, resolved
            FROM system_alerts 
            WHERE created_at > datetime('now', '-7 days')
            ORDER BY created_at DESC
            LIMIT 50
        ''')
        
        alerts = []
        for row in cursor.fetchall():
            alerts.append({
                'type': row[0],
                'severity': row[1],
                'message': row[2],
                'details': row[3],
                'created_at': row[4],
                'resolved': bool(row[5])
            })
        
        # Get alert summary
        cursor.execute('''
            SELECT severity, COUNT(*) as count
            FROM system_alerts 
            WHERE created_at > datetime('now', '-24 hours')
            GROUP BY severity
        ''')
        
        alert_summary = {row[0]: row[1] for row in cursor.fetchall()}
        
        conn.close()
        
        return jsonify({
            'alerts': alerts,
            'summary': alert_summary,
            'total_count': len(alerts)
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    # Start monitoring in background
    start_auto_monitoring()
    
    # Run the unified app with SocketIO on port 8081
    print("Starting Concordance Timeline Dashboard...")
    print("Dashboard: http://127.0.0.1:8081")
    print("Timeline: http://127.0.0.1:8081/timeline")
    print("API: http://127.0.0.1:8081/api/timeline")
    socketio.run(app, debug=True, host='127.0.0.1', port=8081, allow_unsafe_werkzeug=True)
