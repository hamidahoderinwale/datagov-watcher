"""
License Classification and Normalization System
Provides intelligent license detection, classification, and normalization
"""

import re
import logging
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse
import requests
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class LicenseInfo:
    """Structured license information"""
    name: str
    category: str
    url: Optional[str] = None
    description: Optional[str] = None
    is_open: bool = True
    requires_attribution: bool = False
    allows_commercial: bool = True
    allows_derivatives: bool = True
    share_alike: bool = False
    confidence: float = 1.0

class LicenseClassifier:
    """Intelligent license classification and normalization system"""
    
    def __init__(self):
        self.license_patterns = self._initialize_license_patterns()
        self.license_urls = self._initialize_license_urls()
        self.known_licenses = self._initialize_known_licenses()
    
    def _initialize_license_patterns(self) -> Dict[str, Dict]:
        """Initialize regex patterns for license detection"""
        return {
            'public_domain': {
                'patterns': [
                    r'public\s+domain',
                    r'publicdomain',
                    r'pd\b',
                    r'no\s+copyright',
                    r'unrestricted',
                    r'free\s+to\s+use'
                ],
                'category': 'public_domain',
                'is_open': True,
                'requires_attribution': False,
                'allows_commercial': True,
                'allows_derivatives': True
            },
            'cc0': {
                'patterns': [
                    r'cc0',
                    r'creative\s+commons\s+zero',
                    r'cc\s+0',
                    r'creative\s+commons\s+public\s+domain'
                ],
                'category': 'cc0',
                'is_open': True,
                'requires_attribution': False,
                'allows_commercial': True,
                'allows_derivatives': True
            },
            'cc_by': {
                'patterns': [
                    r'cc\s+by(?!\s+(sa|nc))',
                    r'creative\s+commons\s+attribution(?!\s+(share|non))',
                    r'attribution\s+only'
                ],
                'category': 'cc_by',
                'is_open': True,
                'requires_attribution': True,
                'allows_commercial': True,
                'allows_derivatives': True
            },
            'cc_by_sa': {
                'patterns': [
                    r'cc\s+by\s*[-_]?sa',
                    r'cc\s+by\s+share\s+alike',
                    r'creative\s+commons\s+attribution\s+share\s+alike'
                ],
                'category': 'cc_by_sa',
                'is_open': True,
                'requires_attribution': True,
                'allows_commercial': True,
                'allows_derivatives': True,
                'share_alike': True
            },
            'cc_by_nc': {
                'patterns': [
                    r'cc\s+by\s*[-_]?nc(?!\s+sa)',
                    r'cc\s+by\s+non\s*[-_]?commercial(?!\s+share)',
                    r'creative\s+commons\s+attribution\s+non\s*[-_]?commercial'
                ],
                'category': 'cc_by_nc',
                'is_open': True,
                'requires_attribution': True,
                'allows_commercial': False,
                'allows_derivatives': True
            },
            'cc_by_nc_sa': {
                'patterns': [
                    r'cc\s+by\s*[-_]?nc\s*[-_]?sa',
                    r'cc\s+by\s+non\s*[-_]?commercial\s+share\s+alike',
                    r'creative\s+commons\s+attribution\s+non\s*[-_]?commercial\s+share\s+alike'
                ],
                'category': 'cc_by_nc_sa',
                'is_open': True,
                'requires_attribution': True,
                'allows_commercial': False,
                'allows_derivatives': True,
                'share_alike': True
            },
            'mit': {
                'patterns': [
                    r'mit\s+license',
                    r'massachusetts\s+institute\s+of\s+technology',
                    r'mit\b'
                ],
                'category': 'mit',
                'is_open': True,
                'requires_attribution': True,
                'allows_commercial': True,
                'allows_derivatives': True
            },
            'apache': {
                'patterns': [
                    r'apache\s+license',
                    r'apache\s+2\.0',
                    r'apache\s+software\s+foundation'
                ],
                'category': 'apache',
                'is_open': True,
                'requires_attribution': True,
                'allows_commercial': True,
                'allows_derivatives': True
            },
            'gpl': {
                'patterns': [
                    r'gpl\s+v?(\d+)?',
                    r'gnu\s+general\s+public\s+license',
                    r'general\s+public\s+license'
                ],
                'category': 'gpl',
                'is_open': True,
                'requires_attribution': True,
                'allows_commercial': True,
                'allows_derivatives': True,
                'share_alike': True
            },
            'bsd': {
                'patterns': [
                    r'bsd\s+license',
                    r'berkeley\s+software\s+distribution',
                    r'bsd\s+[23]?\.?[01]?'
                ],
                'category': 'bsd',
                'is_open': True,
                'requires_attribution': True,
                'allows_commercial': True,
                'allows_derivatives': True
            },
            'proprietary': {
                'patterns': [
                    r'proprietary',
                    r'all\s+rights\s+reserved',
                    r'copyright',
                    r'commercial\s+license',
                    r'private\s+use\s+only'
                ],
                'category': 'proprietary',
                'is_open': False,
                'requires_attribution': True,
                'allows_commercial': False,
                'allows_derivatives': False
            }
        }
    
    def _initialize_license_urls(self) -> Dict[str, str]:
        """Initialize known license URLs"""
        return {
            'https://creativecommons.org/publicdomain/zero/1.0/': 'cc0',
            'https://creativecommons.org/licenses/by/4.0/': 'cc_by',
            'https://creativecommons.org/licenses/by-sa/4.0/': 'cc_by_sa',
            'https://creativecommons.org/licenses/by-nc/4.0/': 'cc_by_nc',
            'https://creativecommons.org/licenses/by-nc-sa/4.0/': 'cc_by_nc_sa',
            'https://opensource.org/licenses/MIT': 'mit',
            'https://opensource.org/licenses/Apache-2.0': 'apache',
            'https://opensource.org/licenses/GPL-3.0': 'gpl',
            'https://opensource.org/licenses/BSD-3-Clause': 'bsd',
        }
    
    def _initialize_known_licenses(self) -> Dict[str, LicenseInfo]:
        """Initialize known license information"""
        return {
            'public_domain': LicenseInfo(
                name='Public Domain',
                category='public_domain',
                description='No copyright restrictions',
                is_open=True,
                requires_attribution=False,
                allows_commercial=True,
                allows_derivatives=True
            ),
            'cc0': LicenseInfo(
                name='CC0 1.0 Universal',
                category='cc0',
                url='https://creativecommons.org/publicdomain/zero/1.0/',
                description='Creative Commons Public Domain Dedication',
                is_open=True,
                requires_attribution=False,
                allows_commercial=True,
                allows_derivatives=True
            ),
            'cc_by': LicenseInfo(
                name='CC BY 4.0',
                category='cc_by',
                url='https://creativecommons.org/licenses/by/4.0/',
                description='Creative Commons Attribution 4.0 International',
                is_open=True,
                requires_attribution=True,
                allows_commercial=True,
                allows_derivatives=True
            ),
            'cc_by_sa': LicenseInfo(
                name='CC BY-SA 4.0',
                category='cc_by_sa',
                url='https://creativecommons.org/licenses/by-sa/4.0/',
                description='Creative Commons Attribution-ShareAlike 4.0 International',
                is_open=True,
                requires_attribution=True,
                allows_commercial=True,
                allows_derivatives=True,
                share_alike=True
            ),
            'cc_by_nc': LicenseInfo(
                name='CC BY-NC 4.0',
                category='cc_by_nc',
                url='https://creativecommons.org/licenses/by-nc/4.0/',
                description='Creative Commons Attribution-NonCommercial 4.0 International',
                is_open=True,
                requires_attribution=True,
                allows_commercial=False,
                allows_derivatives=True
            ),
            'cc_by_nc_sa': LicenseInfo(
                name='CC BY-NC-SA 4.0',
                category='cc_by_nc_sa',
                url='https://creativecommons.org/licenses/by-nc-sa/4.0/',
                description='Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International',
                is_open=True,
                requires_attribution=True,
                allows_commercial=False,
                allows_derivatives=True,
                share_alike=True
            ),
            'mit': LicenseInfo(
                name='MIT License',
                category='mit',
                url='https://opensource.org/licenses/MIT',
                description='MIT License',
                is_open=True,
                requires_attribution=True,
                allows_commercial=True,
                allows_derivatives=True
            ),
            'apache': LicenseInfo(
                name='Apache License 2.0',
                category='apache',
                url='https://opensource.org/licenses/Apache-2.0',
                description='Apache License 2.0',
                is_open=True,
                requires_attribution=True,
                allows_commercial=True,
                allows_derivatives=True
            ),
            'gpl': LicenseInfo(
                name='GNU GPL',
                category='gpl',
                url='https://opensource.org/licenses/GPL-3.0',
                description='GNU General Public License',
                is_open=True,
                requires_attribution=True,
                allows_commercial=True,
                allows_derivatives=True,
                share_alike=True
            ),
            'bsd': LicenseInfo(
                name='BSD License',
                category='bsd',
                url='https://opensource.org/licenses/BSD-3-Clause',
                description='BSD 3-Clause License',
                is_open=True,
                requires_attribution=True,
                allows_commercial=True,
                allows_derivatives=True
            ),
            'proprietary': LicenseInfo(
                name='Proprietary',
                category='proprietary',
                description='Proprietary license with restrictions',
                is_open=False,
                requires_attribution=True,
                allows_commercial=False,
                allows_derivatives=False
            ),
            'unknown': LicenseInfo(
                name='Unknown License',
                category='unknown',
                description='License information not available or unclear',
                is_open=False,
                requires_attribution=True,
                allows_commercial=False,
                allows_derivatives=False,
                confidence=0.0
            )
        }
    
    def classify_license(self, license_text: str, license_url: Optional[str] = None) -> LicenseInfo:
        """
        Classify a license based on text and/or URL
        
        Args:
            license_text: Raw license text from API
            license_url: Optional license URL
            
        Returns:
            LicenseInfo object with classified license information
        """
        if not license_text and not license_url:
            return self.known_licenses['unknown']
        
        # First, try to match by URL if provided
        if license_url:
            url_match = self._match_license_url(license_url)
            if url_match:
                return self.known_licenses[url_match]
        
        # Then try to match by text patterns
        if license_text:
            text_match = self._match_license_text(license_text)
            if text_match:
                return self.known_licenses[text_match]
        
        # If no match found, return unknown
        return self.known_licenses['unknown']
    
    def _match_license_url(self, url: str) -> Optional[str]:
        """Match license by URL"""
        # Normalize URL
        url = url.lower().strip()
        
        # Direct match
        if url in self.license_urls:
            return self.license_urls[url]
        
        # Partial match for common patterns
        for known_url, license_type in self.license_urls.items():
            if known_url in url or url in known_url:
                return license_type
        
        return None
    
    def _match_license_text(self, text: str) -> Optional[str]:
        """Match license by text patterns"""
        if not text:
            return None
        
        text = text.lower().strip()
        
        # Try each license pattern
        for license_type, config in self.license_patterns.items():
            for pattern in config['patterns']:
                if re.search(pattern, text, re.IGNORECASE):
                    return license_type
        
        return None
    
    def normalize_license(self, license_text: str, license_url: Optional[str] = None) -> Dict:
        """
        Normalize license information into a standardized format
        
        Args:
            license_text: Raw license text
            license_url: Optional license URL
            
        Returns:
            Dictionary with normalized license information
        """
        license_info = self.classify_license(license_text, license_url)
        
        return {
            'name': license_info.name,
            'category': license_info.category,
            'url': license_info.url,
            'description': license_info.description,
            'is_open': license_info.is_open,
            'requires_attribution': license_info.requires_attribution,
            'allows_commercial': license_info.allows_commercial,
            'allows_derivatives': license_info.allows_derivatives,
            'share_alike': license_info.share_alike,
            'confidence': license_info.confidence,
            'raw_text': license_text,
            'raw_url': license_url
        }
    
    def get_license_categories(self) -> List[str]:
        """Get list of all license categories"""
        return list(self.known_licenses.keys())
    
    def get_open_licenses(self) -> List[str]:
        """Get list of open license categories"""
        return [cat for cat, info in self.known_licenses.items() if info.is_open]
    
    def get_license_info(self, category: str) -> Optional[LicenseInfo]:
        """Get detailed information about a specific license category"""
        return self.known_licenses.get(category)

# Global instance
license_classifier = LicenseClassifier()


