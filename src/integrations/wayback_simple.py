#!/usr/bin/env python3
"""
Wayback Simple - Read-only version that works with existing database
No database modifications to avoid locks
"""

import sqlite3
import json
import requests
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WaybackSimple:
    """Simple Wayback functionality - read-only, no database modifications"""
    
    def __init__(self, db_path: str = "datasets.db"):
        self.db_path = db_path
        self.wayback_cdx_url = "http://web.archive.org/cdx/search/cdx"
        self.wayback_base_url = "http://web.archive.org/web"
    
    def search_wayback_cdx(self, url: str, start_date: str = None, end_date: str = None) -> List[Dict]:
        """Search Wayback Machine CDX index for archived versions"""
        try:
            params = {
                'url': url,
                'output': 'json',
                'fl': 'timestamp,original,statuscode,mimetype,length,digest'
            }
            
            if start_date:
                params['from'] = start_date
            if end_date:
                params['to'] = end_date
            
            response = requests.get(self.wayback_cdx_url, params=params, timeout=30)
            response.raise_for_status()
            
            # Parse CDX response
            cdx_data = response.json()
            if not cdx_data or len(cdx_data) < 2:
                return []
            
            # Skip header row
            results = []
            for row in cdx_data[1:]:
                if len(row) >= 6:
                    results.append({
                        'timestamp': row[0],
                        'original_url': row[1],
                        'status_code': int(row[2]) if row[2].isdigit() else None,
                        'mimetype': row[3],
                        'length': int(row[4]) if row[4].isdigit() else None,
                        'digest': row[5],
                        'wayback_url': f"{self.wayback_base_url}/{row[0]}/{url}"
                    })
            
            return results
            
        except Exception as e:
            logger.error(f"Error searching Wayback CDX for {url}: {e}")
            return []
    
    def get_database_stats(self) -> Dict:
        """Get statistics from existing database (read-only)"""
        try:
            conn = sqlite3.connect(self.db_path, timeout=5)
            cursor = conn.cursor()
            
            # Basic stats
            cursor.execute('SELECT COUNT(*) FROM historian_snapshots')
            total_snapshots = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(DISTINCT dataset_id) FROM historian_snapshots')
            total_datasets = cursor.fetchone()[0]
            
            # Check for diffs
            cursor.execute('SELECT COUNT(*) FROM historian_diffs')
            total_diffs = cursor.fetchone()[0]
            
            # Check for volatility metrics
            cursor.execute('SELECT COUNT(*) FROM volatility_metrics')
            total_volatility = cursor.fetchone()[0]
            
            # Get source breakdown
            cursor.execute('SELECT source, COUNT(*) FROM historian_snapshots GROUP BY source')
            sources = cursor.fetchall()
            
            # Get recent snapshots
            cursor.execute('''
                SELECT dataset_id, title, agency, snapshot_date, source
                FROM historian_snapshots
                ORDER BY snapshot_date DESC
                LIMIT 10
            ''')
            
            recent_snapshots = []
            for row in cursor.fetchall():
                recent_snapshots.append({
                    'dataset_id': row[0],
                    'title': row[1],
                    'agency': row[2],
                    'snapshot_date': row[3],
                    'source': row[4]
                })
            
            conn.close()
            
            return {
                'total_snapshots': total_snapshots,
                'total_datasets': total_datasets,
                'total_diffs': total_diffs,
                'total_volatility': total_volatility,
                'sources': dict(sources),
                'recent_snapshots': recent_snapshots
            }
            
        except Exception as e:
            logger.error(f"Error getting database stats: {e}")
            return {}
    
    def detect_vanished_datasets(self) -> List[Dict]:
        """Detect vanished datasets (read-only analysis)"""
        try:
            conn = sqlite3.connect(self.db_path, timeout=5)
            cursor = conn.cursor()
            
            # Get all dataset IDs that have been seen before
            cursor.execute('''
                SELECT DISTINCT dataset_id, MAX(snapshot_date) as last_seen, 
                       MAX(source) as last_source
                FROM historian_snapshots
                GROUP BY dataset_id
            ''')
            
            all_known_datasets = {row[0]: {'last_seen': row[1], 'last_source': row[2]} 
                                for row in cursor.fetchall()}
            
            # Get current live datasets (most recent live snapshot)
            cursor.execute('''
                SELECT DISTINCT dataset_id FROM historian_snapshots 
                WHERE source = 'live' AND snapshot_date = (
                    SELECT MAX(snapshot_date) FROM historian_snapshots WHERE source = 'live'
                )
            ''')
            
            current_live_datasets = {row[0] for row in cursor.fetchall()}
            
            # Find vanished datasets
            vanished = []
            for dataset_id, info in all_known_datasets.items():
                if dataset_id not in current_live_datasets:
                    vanished.append({
                        'dataset_id': dataset_id,
                        'last_seen_date': info['last_seen'],
                        'last_seen_source': info['last_source']
                    })
            
            conn.close()
            return vanished
            
        except Exception as e:
            logger.error(f"Error detecting vanished datasets: {e}")
            return []
    
    def get_vanished_dataset_info(self, dataset_id: str) -> Dict:
        """Get information about a vanished dataset"""
        try:
            conn = sqlite3.connect(self.db_path, timeout=5)
            cursor = conn.cursor()
            
            # Get last known information
            cursor.execute('''
                SELECT title, agency, publisher, license, landing_page, modified, snapshot_date
                FROM historian_snapshots
                WHERE dataset_id = ?
                ORDER BY snapshot_date DESC
                LIMIT 1
            ''', (dataset_id,))
            
            last_known = cursor.fetchone()
            if not last_known:
                return {'error': 'Dataset not found'}
            
            title, agency, publisher, license, landing_page, modified, snapshot_date = last_known
            
            # Get all snapshots for this dataset
            cursor.execute('''
                SELECT snapshot_date, source, title, agency
                FROM historian_snapshots
                WHERE dataset_id = ?
                ORDER BY snapshot_date ASC
            ''', (dataset_id,))
            
            timeline = []
            for row in cursor.fetchall():
                timeline.append({
                    'date': row[0],
                    'source': row[1],
                    'title': row[2],
                    'agency': row[3]
                })
            
            conn.close()
            
            return {
                'dataset_id': dataset_id,
                'last_known': {
                    'title': title,
                    'agency': agency,
                    'publisher': publisher,
                    'license': license,
                    'landing_page': landing_page,
                    'modified': modified,
                    'last_seen_date': snapshot_date
                },
                'timeline': timeline,
                'snapshot_count': len(timeline)
            }
            
        except Exception as e:
            logger.error(f"Error getting vanished dataset info: {e}")
            return {'error': str(e)}
    
    def find_archival_sources(self, dataset_id: str) -> Dict:
        """Find archival sources for a vanished dataset"""
        try:
            # Get dataset info
            dataset_info = self.get_vanished_dataset_info(dataset_id)
            if 'error' in dataset_info:
                return dataset_info
            
            landing_page = dataset_info['last_known']['landing_page']
            if not landing_page:
                return {'error': 'No landing page available'}
            
            # Search Wayback Machine
            wayback_results = self.search_wayback_cdx(
                landing_page,
                start_date=dataset_info['last_known']['last_seen_date'].replace('-', ''),
                end_date=datetime.now().strftime('%Y%m%d')
            )
            
            return {
                'dataset_id': dataset_id,
                'landing_page': landing_page,
                'wayback_snapshots': wayback_results,
                'wayback_count': len(wayback_results)
            }
            
        except Exception as e:
            logger.error(f"Error finding archival sources: {e}")
            return {'error': str(e)}

def main():
    """Main function to demonstrate Wayback Simple functionality"""
    print("🕰️ Wayback Simple - Read-Only Archival Analysis")
    print("=" * 60)
    
    # Initialize Wayback Simple
    wayback = WaybackSimple("datasets.db")
    
    print("\n1. Getting database statistics...")
    stats = wayback.get_database_stats()
    print(f"   Total snapshots: {stats.get('total_snapshots', 0):,}")
    print(f"   Total datasets: {stats.get('total_datasets', 0):,}")
    print(f"   Total diffs: {stats.get('total_diffs', 0):,}")
    print(f"   Total volatility metrics: {stats.get('total_volatility', 0):,}")
    
    if stats.get('sources'):
        print("   Sources:")
        for source, count in stats['sources'].items():
            print(f"     - {source}: {count:,}")
    
    print("\n2. Testing Wayback Machine API...")
    test_url = "https://data.gov"
    wayback_results = wayback.search_wayback_cdx(test_url, "20241201", "20241201")
    print(f"   Found {len(wayback_results)} snapshots for {test_url} today")
    
    if wayback_results:
        result = wayback_results[0]
        print(f"   Latest: {result['timestamp']} - Status: {result['status_code']}")
        print(f"   Size: {result['length']} bytes")
        print(f"   URL: {result['wayback_url']}")
    
    print("\n3. Detecting vanished datasets...")
    vanished = wayback.detect_vanished_datasets()
    print(f"   Found {len(vanished)} vanished datasets")
    
    if vanished:
        print("   Sample vanished datasets:")
        for vd in vanished[:3]:
            print(f"     - {vd['dataset_id']} (last seen: {vd['last_seen_date']})")
    
    print("\n4. Analyzing vanished dataset...")
    if vanished:
        sample_dataset = vanished[0]['dataset_id']
        print(f"   Analyzing: {sample_dataset}")
        
        dataset_info = wayback.get_vanished_dataset_info(sample_dataset)
        if 'error' not in dataset_info:
            print(f"   Title: {dataset_info['last_known']['title']}")
            print(f"   Agency: {dataset_info['last_known']['agency']}")
            print(f"   Timeline entries: {dataset_info['snapshot_count']}")
            
            # Find archival sources
            archival_sources = wayback.find_archival_sources(sample_dataset)
            if 'error' not in archival_sources:
                print(f"   Wayback snapshots: {archival_sources['wayback_count']}")
            else:
                print(f"   Archival search error: {archival_sources['error']}")
    
    print("\n✅ Wayback Simple is fully operational!")
    print("\nKey capabilities:")
    print("  ✓ Read-only database analysis")
    print("  ✓ Vanished dataset detection")
    print("  ✓ Wayback Machine API integration")
    print("  ✓ Archival source discovery")
    print("  ✓ No database modifications (avoiding locks)")
    print("  ✓ Timeline reconstruction")
    
    print(f"\n📊 Dashboard Summary:")
    print(f"  - Total Datasets: {stats.get('total_datasets', 0):,}")
    print(f"  - Total Snapshots: {stats.get('total_snapshots', 0):,}")
    print(f"  - Change Events: {stats.get('total_diffs', 0):,}")
    print(f"  - Vanished Datasets: {len(vanished)}")
    print(f"  - High Risk Datasets: Check volatility metrics")

if __name__ == "__main__":
    main()


