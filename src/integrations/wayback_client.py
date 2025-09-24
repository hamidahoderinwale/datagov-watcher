"""
Wayback Machine client for checking last-seen dates
"""
import requests
import json
from typing import Optional, Dict
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class WaybackClient:
    def __init__(self):
        self.base_url = "http://web.archive.org/cdx/search/cdx"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Data.gov Monitor/1.0 (Educational Research)'
        })
    
    def get_last_snapshot(self, url: str) -> Optional[Dict]:
        """
        Get the last snapshot date for a URL from Wayback Machine
        """
        try:
            params = {
                'url': url,
                'output': 'json',
                'limit': 1,
                'filter': 'statuscode:200'
            }
            
            response = self.session.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if len(data) > 1:  # First row is headers
                snapshot = data[1]  # Get first (most recent) snapshot
                return {
                    'url': snapshot[2],  # original URL
                    'timestamp': snapshot[1],  # timestamp
                    'status': snapshot[4],  # status code
                    'archive_url': f"https://web.archive.org/web/{snapshot[1]}/{snapshot[2]}"
                }
            
            return None
            
        except Exception as e:
            logger.warning(f"Failed to get Wayback data for {url}: {e}")
            return None
    
    def check_url_availability(self, url: str) -> Dict:
        """
        Check if a URL is currently available
        """
        try:
            response = self.session.head(url, timeout=10, allow_redirects=True)
            return {
                'url': url,
                'status_code': response.status_code,
                'available': response.status_code == 200,
                'last_checked': datetime.now().isoformat()
            }
        except Exception as e:
            return {
                'url': url,
                'status_code': None,
                'available': False,
                'error': str(e),
                'last_checked': datetime.now().isoformat()
            }
    
    def enrich_vanished_dataset(self, dataset: Dict) -> Dict:
        """
        Enrich a vanished dataset with Wayback Machine data
        """
        url = dataset.get('original_url', '')
        if not url:
            return dataset
        
        # Check current availability
        availability = self.check_url_availability(url)
        dataset['current_status'] = availability
        
        # Get last snapshot if URL is not available
        if not availability['available']:
            snapshot = self.get_last_snapshot(url)
            if snapshot:
                dataset['last_wayback_snapshot'] = snapshot
                dataset['last_seen_date'] = self._format_timestamp(snapshot['timestamp'])
        
        return dataset
    
    def _format_timestamp(self, timestamp: str) -> str:
        """
        Format Wayback timestamp to readable date
        """
        try:
            # Wayback timestamps are in format YYYYMMDDHHMMSS
            dt = datetime.strptime(timestamp, '%Y%m%d%H%M%S')
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except:
            return timestamp
