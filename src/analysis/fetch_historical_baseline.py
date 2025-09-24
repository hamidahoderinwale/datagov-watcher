#!/usr/bin/env python3
"""
Fetch historical Data.gov snapshots from Wayback Machine to create baseline for vanished dataset detection.

This script:
1. Queries Wayback Machine CDX API for historical snapshots of inventory.data.gov/data.json
2. Fetches a historical snapshot (e.g., from 2020-2022)
3. Compares with current live data to find vanished datasets
4. Populates the vanished_datasets table with real data
"""

import requests
import json
import sqlite3
from datetime import datetime, timedelta
import time
from typing import List, Dict, Set
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HistoricalBaselineFetcher:
    def __init__(self, db_path: str = "datasets.db"):
        self.db_path = db_path
        self.wayback_cdx_url = "https://web.archive.org/cdx/search/cdx"
        self.current_catalog_url = "https://inventory.data.gov/data.json"
        
    def get_historical_snapshots(self, target_url: str, limit: int = 10) -> List[Dict]:
        """Get historical snapshots from Wayback Machine CDX API"""
        logger.info(f"Fetching historical snapshots for {target_url}")
        
        # Try different URL formats that might be archived
        urls_to_try = [
            "https://catalog.data.gov/api/3/action/package_list",
            "https://inventory.data.gov/data.json", 
            "https://data.gov/data.json",
            "https://catalog.data.gov/data.json",
            target_url
        ]
        
        for url in urls_to_try:
            logger.info(f"Trying URL: {url}")
            # Try different time periods for each URL
            time_periods = [
                ('20180101', '20231231'),  # 2018-2023
                ('20190101', '20221231'),  # 2019-2022
                ('20200101', '20211231'),  # 2020-2021
                ('20210101', '20221231'),  # 2021-2022
            ]
            
            for from_date, to_date in time_periods:
                logger.info(f"  Trying period {from_date} to {to_date}")
                params = {
                    'url': url,
                    'output': 'json',
                    'limit': limit,
                    'from': from_date,
                    'to': to_date,
                    'filter': 'statuscode:200',
                    'collapse': 'timestamp:8'  # One snapshot per day
                }
                
                try:
                    response = requests.get(self.wayback_cdx_url, params=params, timeout=30)
                    response.raise_for_status()
                    
                    data = response.json()
                    if len(data) > 1:  # More than just header
                        logger.info(f"Found snapshots for {url} in period {from_date}-{to_date}")
                        
                        # Skip header row
                        snapshots = []
                        for row in data[1:]:
                            if len(row) >= 4:
                                snapshots.append({
                                    'urlkey': row[0],
                                    'timestamp': row[1],
                                    'original': row[2],
                                    'mimetype': row[3],
                                    'statuscode': row[4] if len(row) > 4 else '200',
                                    'digest': row[5] if len(row) > 5 else '',
                                    'length': row[6] if len(row) > 6 else '0'
                                })
                        
                        logger.info(f"Found {len(snapshots)} historical snapshots")
                        return snapshots
                    else:
                        logger.info(f"No snapshots found for {url} in period {from_date}-{to_date}")
                        continue
                        
                except Exception as e:
                    logger.warning(f"Error with {url} in period {from_date}-{to_date}: {e}")
                    continue
        
        logger.error("No snapshots found for any URL")
        return []
    
    def fetch_historical_catalog(self, snapshot: Dict) -> Dict:
        """Fetch the actual catalog data from a historical snapshot"""
        wayback_url = f"https://web.archive.org/web/{snapshot['timestamp']}/{snapshot['original']}"
        logger.info(f"Fetching historical catalog from {wayback_url}")
        
        try:
            response = requests.get(wayback_url, timeout=60)
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"Successfully fetched historical catalog with {len(data.get('dataset', []))} datasets")
            return data
            
        except Exception as e:
            logger.error(f"Error fetching historical catalog: {e}")
            return {}
    
    def fetch_current_catalog(self) -> Dict:
        """Fetch current live catalog"""
        logger.info("Fetching current live catalog")
        
        try:
            response = requests.get(self.current_catalog_url, timeout=60)
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"Successfully fetched current catalog with {len(data.get('dataset', []))} datasets")
            return data
            
        except Exception as e:
            logger.error(f"Error fetching current catalog: {e}")
            return {}
    
    def extract_dataset_ids(self, catalog_data: Dict) -> Set[str]:
        """Extract dataset IDs from catalog data"""
        dataset_ids = set()
        
        for dataset in catalog_data.get('dataset', []):
            # Try different ID fields
            dataset_id = (dataset.get('identifier') or 
                         dataset.get('id') or 
                         dataset.get('@id', '').split('/')[-1])
            
            if dataset_id:
                dataset_ids.add(dataset_id)
        
        return dataset_ids
    
    def extract_dataset_metadata(self, catalog_data: Dict) -> Dict[str, Dict]:
        """Extract dataset metadata for vanished datasets"""
        metadata = {}
        
        for dataset in catalog_data.get('dataset', []):
            dataset_id = (dataset.get('identifier') or 
                         dataset.get('id') or 
                         dataset.get('@id', '').split('/')[-1])
            
            if dataset_id:
                metadata[dataset_id] = {
                    'title': dataset.get('title', 'Unknown'),
                    'publisher': dataset.get('publisher', {}).get('name', 'Unknown') if isinstance(dataset.get('publisher'), dict) else dataset.get('publisher', 'Unknown'),
                    'url': dataset.get('landingPage', dataset.get('url', '')),
                    'modified': dataset.get('modified', ''),
                    'description': dataset.get('description', ''),
                    'organization': dataset.get('organization', {}).get('title', 'Unknown') if isinstance(dataset.get('organization'), dict) else dataset.get('organization', 'Unknown')
                }
        
        return metadata
    
    def find_vanished_datasets(self, historical_catalog: Dict, current_catalog: Dict) -> List[Dict]:
        """Find datasets that exist in historical but not in current catalog"""
        logger.info("Comparing historical vs current catalogs")
        
        # Extract IDs
        historical_ids = self.extract_dataset_ids(historical_catalog)
        current_ids = self.extract_dataset_ids(current_catalog)
        
        logger.info(f"Historical datasets: {len(historical_ids)}")
        logger.info(f"Current datasets: {len(current_ids)}")
        
        # Find vanished IDs
        vanished_ids = historical_ids - current_ids
        logger.info(f"Vanished datasets: {len(vanished_ids)}")
        
        # Get metadata for vanished datasets
        historical_metadata = self.extract_dataset_metadata(historical_catalog)
        
        vanished_datasets = []
        for dataset_id in vanished_ids:
            if dataset_id in historical_metadata:
                meta = historical_metadata[dataset_id]
                vanished_datasets.append({
                    'id': dataset_id,
                    'title': meta['title'],
                    'organization': {'title': meta['organization']},
                    'url': meta['url'],
                    'last_seen': meta['modified'],
                    'suspected_cause': 'Dataset removed from catalog',
                    'archive_url': f"https://web.archive.org/web/*/{meta['url']}" if meta['url'] else '',
                    'wayback_url': f"https://web.archive.org/web/*/{meta['url']}" if meta['url'] else '',
                    'status': 'removed',
                    'publisher': meta['publisher'],
                    'description': meta['description']
                })
        
        return vanished_datasets
    
    def store_vanished_datasets(self, vanished_datasets: List[Dict]):
        """Store vanished datasets in the database"""
        if not vanished_datasets:
            logger.info("No vanished datasets to store")
            return
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create vanished_datasets table if it doesn't exist (matching existing schema)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS vanished_datasets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id TEXT UNIQUE,
                last_seen_date TEXT,
                last_seen_source TEXT,
                disappearance_date TEXT,
                last_known_title TEXT,
                last_known_agency TEXT,
                last_known_landing_page TEXT,
                archival_sources TEXT,
                status TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        stored_count = 0
        for dataset in vanished_datasets:
            try:
                cursor.execute('''
                    INSERT OR REPLACE INTO vanished_datasets
                    (dataset_id, last_seen_date, last_seen_source, disappearance_date,
                     last_known_title, last_known_agency, last_known_landing_page, 
                     archival_sources, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    dataset.get('id', ''),
                    dataset.get('last_seen', ''),
                    'LIL Archive',
                    datetime.now().strftime('%Y-%m-%d'),
                    dataset.get('title', ''),
                    dataset.get('organization', {}).get('title', '') if isinstance(dataset.get('organization'), dict) else dataset.get('organization', ''),
                    dataset.get('url', ''),
                    dataset.get('archive_url', ''),
                    dataset.get('status', 'removed')
                ))
                stored_count += 1
            except Exception as e:
                logger.error(f"Error storing vanished dataset {dataset.get('id', 'unknown')}: {e}")
                continue
        
        conn.commit()
        conn.close()
        
        logger.info(f"Stored {stored_count} vanished datasets in database")
    

    def run_comparison(self):
        """Run the full historical comparison process"""
        logger.info("Starting historical baseline comparison")
        
        # Get historical snapshots
        snapshots = self.get_historical_snapshots(self.current_catalog_url, limit=10)
        if not snapshots:
            logger.error("No historical snapshots found from Wayback Machine")
            logger.info("Trying alternative approach with different time ranges...")
            
            # Try different time ranges
            for year in [2020, 2021, 2022, 2023]:
                logger.info(f"Trying year {year}")
                snapshots = self.get_historical_snapshots(self.current_catalog_url, limit=5)
                if snapshots:
                    break
            
            if not snapshots:
                logger.error("No historical data available from any source")
                return []
        
        # Use the most recent historical snapshot
        historical_snapshot = snapshots[0]
        logger.info(f"Using historical snapshot from {historical_snapshot['timestamp']}")
        
        # Fetch historical catalog
        historical_catalog = self.fetch_historical_catalog(historical_snapshot)
        if not historical_catalog:
            logger.error("Failed to fetch historical catalog")
            return []
        
        # Fetch current catalog
        current_catalog = self.fetch_current_catalog()
        if not current_catalog:
            logger.error("Failed to fetch current catalog")
            return []
        
        # Find vanished datasets
        vanished_datasets = self.find_vanished_datasets(historical_catalog, current_catalog)
        
        if not vanished_datasets:
            logger.info("No vanished datasets found in comparison - all historical datasets still exist")
            return []
        
        # Store results
        self.store_vanished_datasets(vanished_datasets)
        
        logger.info(f"Comparison complete. Found {len(vanished_datasets)} vanished datasets")
        return vanished_datasets

def main():
    """Main function to run the historical baseline fetch"""
    fetcher = HistoricalBaselineFetcher()
    
    try:
        vanished_datasets = fetcher.run_comparison()
        
        if vanished_datasets:
            print(f"\nSuccess Successfully found {len(vanished_datasets)} vanished datasets!")
            print("\nSample vanished datasets:")
            for i, dataset in enumerate(vanished_datasets[:5]):
                print(f"  {i+1}. {dataset['title']} ({dataset['organization'].get('title', 'Unknown')})")
        else:
            print("\n⚠️  No vanished datasets found. This could mean:")
            print("   - All historical datasets still exist")
            print("   - Historical data is not available")
            print("   - There was an error in the comparison")
            
    except Exception as e:
        logger.error(f"Error in main process: {e}")
        print(f"\nError Error: {e}")

if __name__ == "__main__":
    main()
