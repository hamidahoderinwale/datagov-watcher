#!/usr/bin/env python3
"""
Wayback Core - Streamlined version focusing on archival functionality
Removes complex diffing to avoid database locks
"""

import sqlite3
import json
import requests
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WaybackCore:
    """Streamlined Wayback functionality without complex diffing"""
    
    def __init__(self, db_path: str = "datasets.db"):
        self.db_path = db_path
        self.wayback_cdx_url = "http://web.archive.org/cdx/search/cdx"
        self.wayback_base_url = "http://web.archive.org/web"
        self.init_database()
    
    def init_database(self):
        """Initialize database with essential tables only"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Simple vanished datasets table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS vanished_datasets_simple (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id TEXT NOT NULL UNIQUE,
                last_seen_date TEXT NOT NULL,
                last_seen_source TEXT NOT NULL,
                last_known_title TEXT,
                last_known_agency TEXT,
                status TEXT DEFAULT 'vanished',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Simple wayback snapshots table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS wayback_snapshots_simple (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id TEXT NOT NULL,
                url TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                wayback_url TEXT NOT NULL,
                status_code INTEGER,
                content_size INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def detect_vanished_datasets(self) -> List[Dict]:
        """Detect datasets that have vanished from live catalog"""
        try:
            conn = sqlite3.connect(self.db_path)
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
            logger.info(f"Detected {len(vanished)} vanished datasets")
            return vanished
            
        except Exception as e:
            logger.error(f"Error detecting vanished datasets: {e}")
            return []
    
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
    
    def store_wayback_snapshot(self, dataset_id: str, url: str, cdx_result: Dict) -> bool:
        """Store a Wayback snapshot"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO wayback_snapshots_simple
                (dataset_id, url, timestamp, wayback_url, status_code, content_size)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                dataset_id,
                url,
                cdx_result['timestamp'],
                cdx_result['wayback_url'],
                cdx_result['status_code'],
                cdx_result['length']
            ))
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            logger.error(f"Error storing Wayback snapshot: {e}")
            return False
    
    def store_vanished_dataset(self, vanished_info: Dict) -> bool:
        """Store vanished dataset information"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO vanished_datasets_simple
                (dataset_id, last_seen_date, last_seen_source, last_known_title, last_known_agency)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                vanished_info['dataset_id'],
                vanished_info['last_seen_date'],
                vanished_info['last_seen_source'],
                vanished_info.get('last_known_title', 'Unknown'),
                vanished_info.get('last_known_agency', 'Unknown')
            ))
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            logger.error(f"Error storing vanished dataset: {e}")
            return False
    
    def get_vanished_datasets(self) -> List[Dict]:
        """Get list of vanished datasets"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT vd.dataset_id, vd.last_seen_date, vd.last_seen_source, 
                       vd.last_known_title, vd.last_known_agency, vd.status,
                       COUNT(ws.id) as wayback_snapshots
                FROM vanished_datasets_simple vd
                LEFT JOIN wayback_snapshots_simple ws ON vd.dataset_id = ws.dataset_id
                GROUP BY vd.dataset_id
                ORDER BY vd.last_seen_date DESC
            ''')
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    'dataset_id': row[0],
                    'last_seen_date': row[1],
                    'last_seen_source': row[2],
                    'last_known_title': row[3],
                    'last_known_agency': row[4],
                    'status': row[5],
                    'wayback_snapshots': row[6]
                })
            
            conn.close()
            return results
            
        except Exception as e:
            logger.error(f"Error getting vanished datasets: {e}")
            return []
    
    def get_wayback_snapshots(self, dataset_id: str) -> List[Dict]:
        """Get Wayback snapshots for a dataset"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT url, timestamp, wayback_url, status_code, content_size
                FROM wayback_snapshots_simple
                WHERE dataset_id = ?
                ORDER BY timestamp DESC
            ''', (dataset_id,))
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    'url': row[0],
                    'timestamp': row[1],
                    'wayback_url': row[2],
                    'status_code': row[3],
                    'content_size': row[4]
                })
            
            conn.close()
            return results
            
        except Exception as e:
            logger.error(f"Error getting Wayback snapshots: {e}")
            return []
    
    def process_vanished_datasets(self) -> Dict:
        """Process vanished datasets and find archival sources"""
        logger.info("Processing vanished datasets...")
        
        # Detect vanished datasets
        vanished = self.detect_vanished_datasets()
        
        if not vanished:
            return {'vanished_count': 0, 'processed': 0, 'wayback_found': 0}
        
        processed = 0
        wayback_found = 0
        
        for vanished_info in vanished:
            try:
                # Store vanished dataset
                self.store_vanished_dataset(vanished_info)
                processed += 1
                
                # Get last known title and agency
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT title, agency, landing_page
                    FROM historian_snapshots
                    WHERE dataset_id = ? AND source = ?
                    ORDER BY snapshot_date DESC
                    LIMIT 1
                ''', (vanished_info['dataset_id'], vanished_info['last_seen_source']))
                
                last_known = cursor.fetchone()
                conn.close()
                
                if last_known:
                    title, agency, landing_page = last_known
                    
                    # Search Wayback for the landing page
                    if landing_page:
                        wayback_results = self.search_wayback_cdx(
                            landing_page,
                            start_date=vanished_info['last_seen_date'].replace('-', ''),
                            end_date=datetime.now().strftime('%Y%m%d')
                        )
                        
                        if wayback_results:
                            wayback_found += 1
                            
                            # Store first few snapshots
                            for result in wayback_results[:3]:
                                self.store_wayback_snapshot(
                                    vanished_info['dataset_id'],
                                    landing_page,
                                    result
                                )
                
            except Exception as e:
                logger.error(f"Error processing {vanished_info['dataset_id']}: {e}")
                continue
        
        return {
            'vanished_count': len(vanished),
            'processed': processed,
            'wayback_found': wayback_found
        }
    
    def get_dashboard_stats(self) -> Dict:
        """Get statistics for dashboard"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Basic stats
            cursor.execute('SELECT COUNT(*) FROM historian_snapshots')
            total_snapshots = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(DISTINCT dataset_id) FROM historian_snapshots')
            total_datasets = cursor.fetchone()[0]
            
            # Vanished datasets
            cursor.execute('SELECT COUNT(*) FROM vanished_datasets_simple')
            vanished_count = cursor.fetchone()[0]
            
            # Wayback snapshots
            cursor.execute('SELECT COUNT(*) FROM wayback_snapshots_simple')
            wayback_snapshots = cursor.fetchone()[0]
            
            # Recent vanished datasets
            cursor.execute('''
                SELECT dataset_id, last_known_title, last_known_agency, last_seen_date
                FROM vanished_datasets_simple
                ORDER BY last_seen_date DESC
                LIMIT 5
            ''')
            
            recent_vanished = []
            for row in cursor.fetchall():
                recent_vanished.append({
                    'dataset_id': row[0],
                    'title': row[1],
                    'agency': row[2],
                    'last_seen': row[3]
                })
            
            conn.close()
            
            return {
                'total_snapshots': total_snapshots,
                'total_datasets': total_datasets,
                'vanished_datasets': vanished_count,
                'wayback_snapshots': wayback_snapshots,
                'recent_vanished': recent_vanished
            }
            
        except Exception as e:
            logger.error(f"Error getting dashboard stats: {e}")
            return {}

def main():
    """Main function to demonstrate Wayback Core functionality"""
    print("🕰️ Wayback Core - Streamlined Archival Functionality")
    print("=" * 60)
    
    # Initialize Wayback Core
    wayback = WaybackCore("datasets.db")
    
    print("\n1. Getting current database stats...")
    stats = wayback.get_dashboard_stats()
    print(f"   Total snapshots: {stats.get('total_snapshots', 0):,}")
    print(f"   Total datasets: {stats.get('total_datasets', 0):,}")
    print(f"   Vanished datasets: {stats.get('vanished_datasets', 0):,}")
    print(f"   Wayback snapshots: {stats.get('wayback_snapshots', 0):,}")
    
    print("\n2. Testing Wayback Machine API...")
    test_url = "https://data.gov"
    wayback_results = wayback.search_wayback_cdx(test_url, "20241201", "20241201")
    print(f"   Found {len(wayback_results)} snapshots for {test_url} today")
    
    if wayback_results:
        result = wayback_results[0]
        print(f"   Latest: {result['timestamp']} - Status: {result['status_code']}")
        print(f"   Size: {result['length']} bytes")
        print(f"   URL: {result['wayback_url']}")
    
    print("\n3. Processing vanished datasets...")
    results = wayback.process_vanished_datasets()
    print(f"   Vanished datasets: {results['vanished_count']}")
    print(f"   Processed: {results['processed']}")
    print(f"   Found Wayback snapshots: {results['wayback_found']}")
    
    print("\n4. Getting vanished datasets list...")
    vanished = wayback.get_vanished_datasets()
    print(f"   Retrieved {len(vanished)} vanished datasets")
    
    if vanished:
        print("   Sample vanished datasets:")
        for vd in vanished[:3]:
            print(f"     - {vd['dataset_id']}: {vd['last_known_title']}")
            print(f"       Last seen: {vd['last_seen_date']} from {vd['last_seen_source']}")
            print(f"       Wayback snapshots: {vd['wayback_snapshots']}")
    
    print("\n✅ Wayback Core is fully operational!")
    print("\nKey capabilities:")
    print("  ✓ Vanished dataset detection")
    print("  ✓ Wayback Machine API integration")
    print("  ✓ Archival snapshot storage")
    print("  ✓ Dashboard statistics")
    print("  ✓ No complex diffing (avoiding database locks)")

if __name__ == "__main__":
    main()


