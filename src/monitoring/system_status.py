#!/usr/bin/env python3
"""
System status and monitoring script for Data.gov Dataset Monitoring System
"""

import sys
import os
import sqlite3
from datetime import datetime, timedelta
import json

# Add the current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.core.data_fetcher import DataFetcher
from src.analysis.diff_engine import DiffEngine

def get_system_status():
    """Get comprehensive system status"""
    status = {
        'timestamp': datetime.now().isoformat(),
        'database': {},
        'apis': {},
        'datasets': {},
        'reports': {},
        'health': 'unknown'
    }
    
    try:
        # Check database
        if os.path.exists('datasets.db'):
            conn = sqlite3.connect('datasets.db')
            cursor = conn.cursor()
            
            # Get table counts
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            
            status['database']['exists'] = True
            status['database']['tables'] = tables
            
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                status['database'][f'{table}_count'] = count
            
            # Get last check time
            cursor.execute("SELECT MAX(timestamp) FROM monitoring_logs")
            last_check = cursor.fetchone()[0]
            status['database']['last_check'] = last_check
            
            conn.close()
        else:
            status['database']['exists'] = False
        
        # Check API connectivity
        try:
            fetcher = DataFetcher()
            connectivity = fetcher.test_api_connectivity()
            status['apis'] = connectivity
        except Exception as e:
            status['apis'] = {'error': str(e)}
        
        # Get dataset statistics
        try:
            diff_engine = DiffEngine()
            stats = diff_engine.get_comparison_stats()
            status['datasets'] = stats
        except Exception as e:
            status['datasets'] = {'error': str(e)}
        
        # Check reports directory
        if os.path.exists('reports'):
            report_files = os.listdir('reports')
            csv_reports = [f for f in report_files if f.endswith('.csv')]
            html_reports = [f for f in report_files if f.endswith('.html')]
            
            status['reports'] = {
                'directory_exists': True,
                'csv_count': len(csv_reports),
                'html_count': len(html_reports),
                'latest_csv': max(csv_reports) if csv_reports else None,
                'latest_html': max(html_reports) if html_reports else None
            }
        else:
            status['reports'] = {'directory_exists': False}
        
        # Determine overall health
        health_score = 0
        if status['database']['exists']:
            health_score += 1
        if status['apis'].get('datagov_api', False):
            health_score += 1
        if status['datasets'].get('live_datasets', 0) > 0:
            health_score += 1
        if status['reports'].get('directory_exists', False):
            health_score += 1
        
        if health_score >= 3:
            status['health'] = 'healthy'
        elif health_score >= 2:
            status['health'] = 'degraded'
        else:
            status['health'] = 'unhealthy'
        
        return status
        
    except Exception as e:
        status['error'] = str(e)
        status['health'] = 'error'
        return status

def print_status(status):
    """Print formatted system status"""
    print("Data.gov Dataset Monitoring System - Status Report")
    print("=" * 55)
    print(f"Timestamp: {status['timestamp']}")
    print(f"Overall Health: {status['health'].upper()}")
    print()
    
    # Database status
    print("Database Status:")
    if status['database'].get('exists', False):
        print(f"  Database exists")
        print(f"  Tables: {', '.join(status['database'].get('tables', []))}")
        for table in status['database'].get('tables', []):
            count = status['database'].get(f'{table}_count', 0)
            print(f"    - {table}: {count} records")
        print(f"  Last check: {status['database'].get('last_check', 'Never')}")
    else:
        print("  Database not found")
    print()
    
    # API status
    print("API Connectivity:")
    for api, status_val in status['apis'].items():
        if api == 'error':
            print(f"  Error: {status_val}")
        else:
            icon = "OK" if status_val else "FAIL"
            print(f"  {icon} {api.replace('_', ' ').title()}: {'Connected' if status_val else 'Not accessible'}")
    print()
    
    # Dataset statistics
    print("Dataset Statistics:")
    if 'error' in status['datasets']:
        print(f"  Error: {status['datasets']['error']}")
    else:
        print(f"  LIL datasets: {status['datasets'].get('lil_datasets', 0)}")
        print(f"  Live datasets: {status['datasets'].get('live_datasets', 0)}")
        print(f"  Vanished datasets: {status['datasets'].get('vanished_datasets', 0)}")
        print(f"  Last check: {status['datasets'].get('last_check', 'Unknown')}")
    print()
    
    # Reports status
    print("Reports Status:")
    if status['reports'].get('directory_exists', False):
        print(f"  Reports directory exists")
        print(f"  CSV reports: {status['reports'].get('csv_count', 0)}")
        print(f"  HTML reports: {status['reports'].get('html_count', 0)}")
        if status['reports'].get('latest_csv'):
            print(f"  Latest CSV: {status['reports']['latest_csv']}")
        if status['reports'].get('latest_html'):
            print(f"  Latest HTML: {status['reports']['latest_html']}")
    else:
        print("  Reports directory not found")
    print()
    
    # Recommendations
    print("Recommendations:")
    if status['health'] == 'healthy':
        print("  🎉 System is running optimally!")
    elif status['health'] == 'degraded':
        print("  ⚠️  System is partially functional - check API connectivity")
    else:
        print("  🚨 System needs attention - check database and API connections")
    
    if not status['apis'].get('datagov_api', False):
        print("  - Data.gov API is not accessible - check internet connection")
    if not status['apis'].get('lil_api', False):
        print("  - LIL API is not accessible - this is expected if not publicly available")
    if status['datasets'].get('live_datasets', 0) == 0:
        print("  - No live datasets found - run a manual check")

def main():
    """Main function"""
    if len(sys.argv) > 1 and sys.argv[1] == '--json':
        # Output JSON for programmatic use
        status = get_system_status()
        print(json.dumps(status, indent=2))
    else:
        # Output human-readable format
        status = get_system_status()
        print_status(status)

if __name__ == "__main__":
    main()
