#!/usr/bin/env python3
"""
Simple Flask app to demonstrate Harvard LIL data integration
"""

from flask import Flask, render_template, jsonify
import sqlite3
import json
from datetime import datetime

app = Flask(__name__, 
           template_folder='web/templates',
           static_folder='web/static')

def get_database_connection():
    return sqlite3.connect("datasets.db")

@app.route('/')
def index():
    return render_template('pages/postmortem_reports.html')

@app.route('/api/vanished-datasets')
def api_vanished_datasets():
    """API endpoint for vanished datasets"""
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT dataset_id, last_known_title as title, last_known_agency as agency, 
                   last_known_landing_page as original_url, 
                   last_seen_date, archival_sources as archive_url, status, created_at
            FROM vanished_datasets
            ORDER BY last_seen_date DESC
            LIMIT 100
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

@app.route('/api/vanished-datasets/political-analysis')
def api_political_analysis():
    """Analyze political patterns in vanished datasets"""
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Get all vanished datasets
        cursor.execute('''
            SELECT dataset_id, last_known_title, last_known_agency, last_seen_date, status
            FROM vanished_datasets
            ORDER BY last_seen_date DESC
        ''')
        
        vanished_datasets = cursor.fetchall()
        
        # Political analysis
        analysis = {
            'total_vanished': len(vanished_datasets),
            'political_patterns': {
                'post_trump_2017_inauguration': 0,
                'post_biden_inauguration': 0,
                'post_trump_2025_inauguration': 0,
                'affected_agencies_count': 0
            },
            'agency_breakdown': {},
            'content_analysis': {
                'climate_related': 0,
                'dei_related': 0,
                'climate_percentage': 0,
                'dei_percentage': 0
            },
            'timeline_analysis': {}
        }
        
        # Key political dates
        trump_inauguration_2017 = datetime(2017, 1, 20)
        biden_inauguration = datetime(2021, 1, 20)
        trump_inauguration_2025 = datetime(2025, 1, 20)
        
        # Agencies disproportionately affected
        affected_agencies = [
            'Department of Energy',
            'National Oceanic and Atmospheric Administration', 
            'Department of the Interior',
            'NASA',
            'Environmental Protection Agency'
        ]
        
        climate_keywords = ['climate', 'temperature', 'emission', 'carbon', 'greenhouse', 'global warming', 'coral reef', 'thermal']
        dei_keywords = ['diversity', 'equity', 'inclusion', 'minority', 'marginalized', 'disadvantaged']
        
        agency_counts = {}
        climate_related_count = 0
        dei_related_count = 0
        affected_agency_count = 0
        
        for dataset in vanished_datasets:
            dataset_id, title, agency, last_seen_date, status = dataset
            
            # Agency analysis
            if agency:
                agency_counts[agency] = agency_counts.get(agency, 0) + 1
                
                # Check if from affected agency
                if any(affected_agency in agency for affected_agency in affected_agencies):
                    affected_agency_count += 1
            
            # Content analysis
            if title:
                if any(keyword.lower() in title.lower() for keyword in climate_keywords):
                    climate_related_count += 1
                if any(keyword.lower() in title.lower() for keyword in dei_keywords):
                    dei_related_count += 1
            
            # Political timeline analysis
            if last_seen_date:
                try:
                    last_seen = datetime.strptime(last_seen_date, '%Y-%m-%d')
                    days_since_trump_2017 = (last_seen - trump_inauguration_2017).days
                    days_since_biden = (last_seen - biden_inauguration).days
                    days_since_trump_2025 = (last_seen - trump_inauguration_2025).days
                    
                    if abs(days_since_trump_2017) <= 30:
                        analysis['political_patterns']['post_trump_2017_inauguration'] += 1
                    if abs(days_since_biden) <= 30:
                        analysis['political_patterns']['post_biden_inauguration'] += 1
                    if abs(days_since_trump_2025) <= 30:
                        analysis['political_patterns']['post_trump_2025_inauguration'] += 1
                except:
                    pass
        
        # Compile analysis
        analysis['political_patterns']['affected_agencies_count'] = affected_agency_count
        analysis['content_analysis']['climate_related'] = climate_related_count
        analysis['content_analysis']['dei_related'] = dei_related_count
        analysis['content_analysis']['climate_percentage'] = round((climate_related_count / len(vanished_datasets)) * 100, 2) if vanished_datasets else 0
        analysis['content_analysis']['dei_percentage'] = round((dei_related_count / len(vanished_datasets)) * 100, 2) if vanished_datasets else 0
        
        # Top affected agencies
        analysis['agency_breakdown'] = dict(sorted(agency_counts.items(), key=lambda x: x[1], reverse=True)[:10])
        
        conn.close()
        
        return jsonify(analysis)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/load-harvard-data')
def api_load_harvard_data():
    """Load Harvard LIL data to populate vanished datasets"""
    try:
        from src.integrations.harvard_lil_integration import HarvardLILIntegration
        
        harvard_lil = HarvardLILIntegration()
        
        # Load Harvard datasets (limit to 50 for demo)
        datasets = harvard_lil.load_harvard_datasets(50)
        
        # Get updated political analysis
        analysis = harvard_lil.get_political_analysis()
        
        return jsonify({
            'success': True,
            'datasets_loaded': len(datasets),
            'political_analysis': analysis,
            'message': f'Successfully loaded {len(datasets)} vanished datasets from Harvard LIL mirror'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/recovery-sources')
def api_recovery_sources():
    """Get information about all available recovery sources"""
    return jsonify({
        'recovery_sources': {
            'lil': {
                'name': 'Harvard LIL + Source Cooperative',
                'api_url': 'https://source.coop/api/lil-data-gov/search',
                'description': '16 TB Data.gov mirror (311k datasets, 2024–2025)',
                'priority': 1
            },
            'findlostdata': {
                'name': 'Find Lost Data (UMich)',
                'api_url': 'https://findlostdata.org/api/search',
                'description': 'Federated search across Harvard LIL, Data Lumos, IA, Dataverse',
                'priority': 2
            },
            'datalumos': {
                'name': 'DataLumos (ICPSR)',
                'api_url': 'https://www.datalumos.org/api/search',
                'description': 'DOI-assigned open datasets',
                'priority': 3
            },
            'wayback': {
                'name': 'Internet Archive Wayback Machine',
                'api_url': 'https://web.archive.org/cdx/search/cdx',
                'description': 'File-level captures for CSVs, PDFs, APIs',
                'priority': 4
            }
        },
        'total_sources': 4,
        'description': 'Unified Recovery System based on University of Michigan approach'
    })

if __name__ == '__main__':
    print("Starting Simple Dataset State Historian...")
    print("Dashboard: http://127.0.0.1:8081")
    print("Post-mortem Reports: http://127.0.0.1:8081")
    print("")
    
    app.run(host='0.0.0.0', port=8081, debug=True)
