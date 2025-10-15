"""
Unified Dataset Recovery System
Based on University of Michigan "Missing Government Websites and Data" guide
Implements layered retrieval across all major archival sources
"""

import requests
import json
import hashlib
import time
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from urllib.parse import quote, urlparse
from dataclasses import dataclass
from enum import Enum

class RecoveryStatus(Enum):
    FOUND_LIL = "found_lil"
    FOUND_WAYBACK = "found_wayback"
    FOUND_DATALUMOS = "found_datalumos"
    FOUND_EDGI = "found_edgi"
    FOUND_EOT = "found_eot"
    FOUND_STATE = "found_state"
    REISSUED = "reissued"
    MISSING = "missing"
    FOIA_FILED = "foia_filed"

@dataclass
class RecoveryResult:
    status: RecoveryStatus
    source: str
    url: str
    capture_date: Optional[str] = None
    checksum: Optional[str] = None
    metadata: Optional[Dict] = None
    confidence: float = 0.0

@dataclass
class DatasetMetadata:
    title: str
    agency: str
    data_gov_id: str
    landing_url: str
    last_seen: str
    keywords: List[str]
    description: Optional[str] = None

class UnifiedRecoverySystem:
    """
    Comprehensive dataset recovery system implementing the UMich approach
    """
    
    def __init__(self):
        self.recovery_sources = {
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
            },
            'edgi': {
                'name': 'EDGI Climate & Justice',
                'api_url': 'https://envirodatagov.org/api/search',
                'description': 'Environmental/CEJST datasets',
                'priority': 5
            },
            'eot': {
                'name': 'End of Term Archive',
                'api_url': 'https://eotarchive.cdlib.org/api/search',
                'description': 'End-of-term site captures',
                'priority': 6
            },
            'webrecorder': {
                'name': 'Webrecorder US Gov Archive',
                'api_url': 'https://webrecorder.net/collections/us-gov/api/search',
                'description': 'High-fidelity website archives',
                'priority': 7
            },
            'ucsb': {
                'name': 'UCSB Public Data Git',
                'api_url': 'https://api.github.com/search/repositories',
                'description': 'NASA/NOAA/DOE mirrors',
                'priority': 8
            }
        }
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Dataset State Historian Recovery System 1.0'
        })

    def search_dataset(self, dataset_metadata: DatasetMetadata) -> List[RecoveryResult]:
        """
        Search for a dataset across all recovery sources in priority order
        """
        results = []
        
        # Search in priority order
        for source_id, source_info in sorted(
            self.recovery_sources.items(), 
            key=lambda x: x[1]['priority']
        ):
            try:
                result = self._search_source(source_id, dataset_metadata)
                if result:
                    results.append(result)
                    # If we found a high-confidence result, we can stop early
                    if result.confidence > 0.8:
                        break
            except Exception as e:
                print(f"Error searching {source_id}: {e}")
                continue
        
        return results

    def _search_source(self, source_id: str, dataset_metadata: DatasetMetadata) -> Optional[RecoveryResult]:
        """
        Search a specific recovery source
        """
        source_info = self.recovery_sources[source_id]
        
        if source_id == 'lil':
            return self._search_lil(dataset_metadata)
        elif source_id == 'findlostdata':
            return self._search_findlostdata(dataset_metadata)
        elif source_id == 'datalumos':
            return self._search_datalumos(dataset_metadata)
        elif source_id == 'wayback':
            return self._search_wayback(dataset_metadata)
        elif source_id == 'edgi':
            return self._search_edgi(dataset_metadata)
        elif source_id == 'eot':
            return self._search_eot(dataset_metadata)
        elif source_id == 'webrecorder':
            return self._search_webrecorder(dataset_metadata)
        elif source_id == 'ucsb':
            return self._search_ucsb(dataset_metadata)
        
        return None

    def _search_lil(self, dataset_metadata: DatasetMetadata) -> Optional[RecoveryResult]:
        """Search Harvard LIL + Source Cooperative"""
        try:
            # Search by Data.gov ID first
            query = dataset_metadata.data_gov_id
            response = self.session.get(
                f"{self.recovery_sources['lil']['api_url']}?q={quote(query)}",
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get('results'):
                    result = data['results'][0]
                    return RecoveryResult(
                        status=RecoveryStatus.FOUND_LIL,
                        source='Harvard LIL',
                        url=result.get('url', ''),
                        capture_date=result.get('capture_date'),
                        checksum=result.get('checksum'),
                        metadata=result,
                        confidence=0.95
                    )
        except Exception as e:
            print(f"LIL search error: {e}")
        
        return None

    def _search_findlostdata(self, dataset_metadata: DatasetMetadata) -> Optional[RecoveryResult]:
        """Search Find Lost Data metasearch"""
        try:
            query = f"{dataset_metadata.title} {dataset_metadata.agency}"
            response = self.session.get(
                f"{self.recovery_sources['findlostdata']['api_url']}?q={quote(query)}",
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get('datasets'):
                    for dataset in data['datasets']:
                        if self._is_match(dataset_metadata, dataset):
                            return RecoveryResult(
                                status=RecoveryStatus.FOUND_LIL,  # FindLostData aggregates LIL
                                source='Find Lost Data',
                                url=dataset.get('url', ''),
                                capture_date=dataset.get('date'),
                                metadata=dataset,
                                confidence=0.85
                            )
        except Exception as e:
            print(f"FindLostData search error: {e}")
        
        return None

    def _search_datalumos(self, dataset_metadata: DatasetMetadata) -> Optional[RecoveryResult]:
        """Search DataLumos (ICPSR)"""
        try:
            query = dataset_metadata.title
            response = self.session.get(
                f"{self.recovery_sources['datalumos']['api_url']}?q={quote(query)}",
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get('results'):
                    for result in data['results']:
                        if self._is_match(dataset_metadata, result):
                            return RecoveryResult(
                                status=RecoveryStatus.FOUND_DATALUMOS,
                                source='DataLumos',
                                url=result.get('doi_url', result.get('url', '')),
                                capture_date=result.get('publication_date'),
                                metadata=result,
                                confidence=0.9
                            )
        except Exception as e:
            print(f"DataLumos search error: {e}")
        
        return None

    def _search_wayback(self, dataset_metadata: DatasetMetadata) -> Optional[RecoveryResult]:
        """Search Internet Archive Wayback Machine"""
        try:
            url = dataset_metadata.landing_url
            if not url:
                return None
                
            response = self.session.get(
                f"{self.recovery_sources['wayback']['api_url']}?url={quote(url)}*&output=json&fl=timestamp,original",
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                if len(data) > 1:  # Skip header row
                    # Get the most recent capture
                    latest_capture = data[1]  # First data row
                    timestamp = latest_capture[0]
                    original_url = latest_capture[1]
                    
                    # Convert timestamp to readable date
                    capture_date = datetime.strptime(timestamp, '%Y%m%d%H%M%S').strftime('%Y-%m-%d')
                    wayback_url = f"https://web.archive.org/web/{timestamp}/{original_url}"
                    
                    return RecoveryResult(
                        status=RecoveryStatus.FOUND_WAYBACK,
                        source='Wayback Machine',
                        url=wayback_url,
                        capture_date=capture_date,
                        metadata={'timestamp': timestamp, 'original_url': original_url},
                        confidence=0.8
                    )
        except Exception as e:
            print(f"Wayback search error: {e}")
        
        return None

    def _search_edgi(self, dataset_metadata: DatasetMetadata) -> Optional[RecoveryResult]:
        """Search EDGI Climate & Justice"""
        try:
            # EDGI focuses on environmental and justice data
            if not self._is_environmental_or_justice(dataset_metadata):
                return None
                
            query = dataset_metadata.title
            response = self.session.get(
                f"{self.recovery_sources['edgi']['api_url']}?q={quote(query)}",
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get('datasets'):
                    for dataset in data['datasets']:
                        if self._is_match(dataset_metadata, dataset):
                            return RecoveryResult(
                                status=RecoveryStatus.FOUND_EDGI,
                                source='EDGI',
                                url=dataset.get('url', ''),
                                capture_date=dataset.get('date'),
                                metadata=dataset,
                                confidence=0.85
                            )
        except Exception as e:
            print(f"EDGI search error: {e}")
        
        return None

    def _search_eot(self, dataset_metadata: DatasetMetadata) -> Optional[RecoveryResult]:
        """Search End of Term Archive"""
        try:
            query = dataset_metadata.landing_url
            if not query:
                return None
                
            response = self.session.get(
                f"{self.recovery_sources['eot']['api_url']}?url={quote(query)}",
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get('captures'):
                    # Get the most recent capture
                    latest_capture = data['captures'][0]
                    return RecoveryResult(
                        status=RecoveryStatus.FOUND_EOT,
                        source='End of Term Archive',
                        url=latest_capture.get('url', ''),
                        capture_date=latest_capture.get('date'),
                        metadata=latest_capture,
                        confidence=0.8
                    )
        except Exception as e:
            print(f"EOT search error: {e}")
        
        return None

    def _search_webrecorder(self, dataset_metadata: DatasetMetadata) -> Optional[RecoveryResult]:
        """Search Webrecorder US Gov Archive"""
        try:
            query = dataset_metadata.landing_url
            if not query:
                return None
                
            response = self.session.get(
                f"{self.recovery_sources['webrecorder']['api_url']}?url={quote(query)}",
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get('collections'):
                    for collection in data['collections']:
                        if collection.get('urls'):
                            return RecoveryResult(
                                status=RecoveryStatus.FOUND_EOT,  # Similar to EOT
                                source='Webrecorder',
                                url=collection['urls'][0],
                                capture_date=collection.get('date'),
                                metadata=collection,
                                confidence=0.75
                            )
        except Exception as e:
            print(f"Webrecorder search error: {e}")
        
        return None

    def _search_ucsb(self, dataset_metadata: DatasetMetadata) -> Optional[RecoveryResult]:
        """Search UCSB Public Data Git"""
        try:
            # Search GitHub for public data repositories
            query = f"{dataset_metadata.title} site:github.com/publicdata-u-csb"
            response = self.session.get(
                f"{self.recovery_sources['ucsb']['api_url']}?q={quote(query)}",
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get('items'):
                    for item in data['items']:
                        if self._is_match(dataset_metadata, item):
                            return RecoveryResult(
                                status=RecoveryStatus.FOUND_STATE,
                                source='UCSB Public Data',
                                url=item.get('html_url', ''),
                                capture_date=item.get('updated_at', '').split('T')[0],
                                metadata=item,
                                confidence=0.7
                            )
        except Exception as e:
            print(f"UCSB search error: {e}")
        
        return None

    def _is_match(self, dataset_metadata: DatasetMetadata, result: Dict) -> bool:
        """Check if a search result matches the dataset"""
        # Simple matching logic - can be enhanced
        title_match = dataset_metadata.title.lower() in result.get('title', '').lower()
        agency_match = dataset_metadata.agency.lower() in result.get('agency', '').lower()
        
        return title_match or agency_match

    def _is_environmental_or_justice(self, dataset_metadata: DatasetMetadata) -> bool:
        """Check if dataset is environmental or justice-related"""
        env_keywords = ['climate', 'environment', 'emission', 'carbon', 'pollution', 'energy']
        justice_keywords = ['justice', 'equity', 'diversity', 'minority', 'disadvantaged']
        
        text = f"{dataset_metadata.title} {dataset_metadata.description or ''}".lower()
        
        return any(keyword in text for keyword in env_keywords + justice_keywords)

    def generate_provenance_pack(self, dataset_metadata: DatasetMetadata, results: List[RecoveryResult]) -> Dict:
        """Generate comprehensive provenance pack"""
        provenance_pack = {
            "dataset": {
                "title": dataset_metadata.title,
                "agency": dataset_metadata.agency,
                "data_gov_id": dataset_metadata.data_gov_id,
                "original_url": dataset_metadata.landing_url,
                "last_seen": dataset_metadata.last_seen,
                "keywords": dataset_metadata.keywords
            },
            "recovery_results": [],
            "provenance_chain": [],
            "recovery_timestamp": datetime.now().isoformat(),
            "recovery_system": "Unified Recovery System v1.0"
        }
        
        for result in results:
            provenance_pack["recovery_results"].append({
                "status": result.status.value,
                "source": result.source,
                "url": result.url,
                "capture_date": result.capture_date,
                "checksum": result.checksum,
                "confidence": result.confidence,
                "metadata": result.metadata
            })
            
            provenance_pack["provenance_chain"].append(result.source)
        
        return provenance_pack

    def get_recovery_status_badge(self, results: List[RecoveryResult]) -> Tuple[str, str]:
        """Get status badge for UI display"""
        if not results:
            return "🚨", "Missing (FOIA Filed)"
        
        best_result = max(results, key=lambda x: x.confidence)
        
        status_map = {
            RecoveryStatus.FOUND_LIL: ("✅", "Found (LIL Mirror)"),
            RecoveryStatus.FOUND_WAYBACK: ("🕰", "Found (Wayback)"),
            RecoveryStatus.FOUND_DATALUMOS: ("📊", "Found (DataLumos)"),
            RecoveryStatus.FOUND_EDGI: ("🌱", "Found (EDGI)"),
            RecoveryStatus.FOUND_EOT: ("🏛️", "Found (EOT Archive)"),
            RecoveryStatus.FOUND_STATE: ("🏛️", "Found (State Archive)"),
            RecoveryStatus.REISSUED: ("♻️", "Reissued (Substitute Dataset)"),
            RecoveryStatus.MISSING: ("🚨", "Missing (FOIA Filed)"),
            RecoveryStatus.FOIA_FILED: ("📋", "FOIA Filed")
        }
        
        return status_map.get(best_result.status, ("❓", "Unknown"))

# CLI interface for dataset recovery
if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        # Command line usage
        recovery_system = UnifiedRecoverySystem()
        print("Dataset Recovery System - Use the CLI tool for interactive recovery")
        print("Run: python src/recovery/rescue_datasets_cli.py --help")
    else:
        print("Dataset Recovery System initialized successfully")
