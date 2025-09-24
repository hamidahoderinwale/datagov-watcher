"""
Dataset comparison engine to identify vanished datasets
"""
import sqlite3
from typing import List, Dict, Set
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class DiffEngine:
    def __init__(self, db_path: str = "datasets.db"):
        self.db_path = db_path
    
    def find_vanished_datasets(self) -> List[Dict]:
        """
        Compare LIL manifest with live catalog to find vanished datasets
        """
        logger.info("Comparing datasets to find vanished ones...")
        
        # Get datasets from both sources
        lil_datasets = self._get_datasets_by_source("lil")
        live_datasets = self._get_datasets_by_source("live")
        
        # Create sets of dataset IDs for comparison
        lil_ids = {dataset['dataset_id'] for dataset in lil_datasets}
        live_ids = {dataset['dataset_id'] for dataset in live_datasets}
        
        # Find datasets that exist in LIL but not in live
        vanished_ids = lil_ids - live_ids
        
        # Build vanished dataset records
        vanished_datasets = []
        for dataset in lil_datasets:
            if dataset['dataset_id'] in vanished_ids:
                vanished_record = {
                    'id': dataset['dataset_id'],
                    'title': dataset.get('title', 'Unknown'),
                    'organization': {'title': dataset.get('agency', 'Unknown')},
                    'url': dataset.get('url', ''),
                    'last_seen': dataset.get('last_modified', ''),
                    'suspected_cause': self._determine_suspected_cause(dataset),
                    'archive_url': self._get_archive_link(dataset),
                    'wayback_url': self._get_wayback_link(dataset),
                    'status': 'removed'
                }
                vanished_datasets.append(vanished_record)
        
        # Store vanished datasets
        self._store_vanished_datasets(vanished_datasets)
        
        logger.info(f"Found {len(vanished_datasets)} vanished datasets")
        return vanished_datasets
    
    def _get_datasets_by_source(self, source: str) -> List[Dict]:
        """Get datasets from database by source"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        if source == "lil":
            # Get datasets from LIL manifests (archived datasets)
            cursor.execute("""
                SELECT DISTINCT dataset_id, title, publisher as agency, 
                       json_extract(metadata, '$.url') as url,
                       modified as last_modified, created_at
                FROM lil_manifests 
                WHERE dataset_id IS NOT NULL
            """)
        elif source == "live":
            # Get datasets from current live monitoring
            cursor.execute("""
                SELECT DISTINCT dataset_id, title, agency, url, last_modified, created_at
                FROM dataset_states 
                WHERE dataset_id IS NOT NULL
            """)
        else:
            conn.close()
            return []
        
        columns = [description[0] for description in cursor.description]
        datasets = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        conn.close()
        return datasets
    
    def _determine_suspected_cause(self, dataset: Dict) -> str:
        """
        Determine the suspected cause of dataset disappearance
        """
        # Simple heuristic - in real implementation, this would be more sophisticated
        title = dataset.get('title', '').lower()
        agency = dataset.get('agency', '').lower()
        
        if 'covid' in title or 'pandemic' in title:
            return "Policy change - COVID data sunset"
        elif 'climate' in title:
            return "Potential policy takedown"
        elif 'draft' in title:
            return "Draft-only status"
        elif agency in ['cdc', 'fda', 'nih']:
            return "Health data policy change"
        else:
            return "Unknown - requires investigation"
    
    def _get_archive_link(self, dataset: Dict) -> str:
        """
        Generate archive link for vanished dataset
        """
        # In real implementation, this would link to actual LIL archive
        dataset_id = dataset.get('dataset_id', '')
        if dataset_id:
            return f"https://lil.law.harvard.edu/data-gov-archive/{dataset_id}"
        else:
            return ""
    
    def _get_wayback_link(self, dataset: Dict) -> str:
        """
        Generate Wayback Machine link for vanished dataset
        """
        base_url = "https://web.archive.org/web/"
        original_url = dataset.get('url', '')
        
        if original_url:
            return f"{base_url}*/{original_url}"
        else:
            return ""
    
    def _store_vanished_datasets(self, vanished_datasets: List[Dict]):
        """Store vanished datasets in database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        for dataset in vanished_datasets:
            cursor.execute('''
                INSERT OR REPLACE INTO vanished_datasets 
                (id, title, agency, original_url, last_seen_date, suspected_cause, archive_link, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                dataset['id'],
                dataset['title'],
                dataset['agency'],
                dataset['original_url'],
                dataset['last_seen_date'],
                dataset['suspected_cause'],
                dataset['archive_link'],
                dataset['status']
            ))
        
        conn.commit()
        conn.close()
    
    def get_vanished_datasets(self) -> List[Dict]:
        """Get all vanished datasets from database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM vanished_datasets ORDER BY discovered_at DESC")
        columns = [description[0] for description in cursor.description]
        datasets = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        conn.close()
        return datasets
    
    def get_comparison_stats(self) -> Dict:
        """Get statistics about the comparison"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Count datasets by source
        cursor.execute("SELECT source, COUNT(*) FROM datasets GROUP BY source")
        source_counts = dict(cursor.fetchall())
        
        # Count vanished datasets
        cursor.execute("SELECT COUNT(*) FROM vanished_datasets")
        vanished_count = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            'lil_datasets': source_counts.get('lil', 0),
            'live_datasets': source_counts.get('live', 0),
            'vanished_datasets': vanished_count,
            'last_check': datetime.now().isoformat()
        }
