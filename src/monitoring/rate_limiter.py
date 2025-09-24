"""
Rate Limiting and Exponential Backoff Implementation
Handles 429 (Too Many Requests) errors with intelligent retry logic
"""

import asyncio
import time
import random
import logging
from typing import Dict, Optional, Tuple, List
from dataclasses import dataclass
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

@dataclass
class RateLimitInfo:
    """Information about rate limiting for a specific domain"""
    domain: str
    retry_after: Optional[int] = None
    last_429_time: Optional[datetime] = None
    consecutive_429s: int = 0
    backoff_multiplier: float = 1.0
    max_backoff: int = 3600  # 1 hour max backoff
    base_delay: float = 120.0  # 2 minutes base delay for 30/hour rate
    requests_per_hour: int = 30  # 30 requests per hour limit
    request_timestamps: List[datetime] = None  # Track request timestamps
    
    def __post_init__(self):
        if self.request_timestamps is None:
            self.request_timestamps = []

class ExponentialBackoffRateLimiter:
    """
    Intelligent rate limiter with exponential backoff for handling 429 errors
    """
    
    def __init__(self, requests_per_hour: int = 30):
        self.rate_limits: Dict[str, RateLimitInfo] = {}
        self.global_delay = 0.0
        self.max_global_delay = 3600.0  # 1 hour max global delay
        self.requests_per_hour = requests_per_hour
        self.base_delay_seconds = 3600 / requests_per_hour  # 120 seconds for 30/hour
        
    def _get_domain_from_url(self, url: str) -> str:
        """Extract domain from URL for rate limiting"""
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            return parsed.netloc.lower()
        except Exception:
            return "unknown"
    
    def _can_make_request(self, domain: str) -> Tuple[bool, float]:
        """
        Check if we can make a request within the 30/hour limit
        Returns (can_make_request, delay_seconds)
        """
        if domain not in self.rate_limits:
            self.rate_limits[domain] = RateLimitInfo(
                domain=domain,
                requests_per_hour=self.requests_per_hour,
                base_delay=self.base_delay_seconds
            )
        
        rate_limit_info = self.rate_limits[domain]
        now = datetime.now()
        
        # Clean old timestamps (older than 1 hour)
        one_hour_ago = now - timedelta(hours=1)
        rate_limit_info.request_timestamps = [
            ts for ts in rate_limit_info.request_timestamps 
            if ts > one_hour_ago
        ]
        
        # Check if we're under the limit
        if len(rate_limit_info.request_timestamps) < self.requests_per_hour:
            return True, 0.0
        
        # Calculate delay until we can make the next request
        oldest_request = min(rate_limit_info.request_timestamps)
        next_available = oldest_request + timedelta(hours=1)
        delay_seconds = (next_available - now).total_seconds()
        
        return False, max(0, delay_seconds)
    
    def _record_request(self, domain: str):
        """Record a request timestamp for rate limiting"""
        if domain not in self.rate_limits:
            self.rate_limits[domain] = RateLimitInfo(
                domain=domain,
                requests_per_hour=self.requests_per_hour,
                base_delay=self.base_delay_seconds
            )
        
        self.rate_limits[domain].request_timestamps.append(datetime.now())
    
    def _calculate_backoff_delay(self, rate_limit_info: RateLimitInfo) -> float:
        """Calculate exponential backoff delay with jitter"""
        # Exponential backoff: base_delay * (2 ^ consecutive_429s) * multiplier
        exponential_delay = rate_limit_info.base_delay * (2 ** rate_limit_info.consecutive_429s)
        
        # Apply multiplier (can be adjusted based on domain behavior)
        delay = exponential_delay * rate_limit_info.backoff_multiplier
        
        # Cap at maximum backoff
        delay = min(delay, rate_limit_info.max_backoff)
        
        # Add jitter (±25% randomization to prevent thundering herd)
        jitter = random.uniform(0.75, 1.25)
        delay *= jitter
        
        return delay
    
    def _update_rate_limit_info(self, domain: str, status_code: int, retry_after: Optional[int] = None):
        """Update rate limiting information based on response"""
        if domain not in self.rate_limits:
            self.rate_limits[domain] = RateLimitInfo(domain=domain)
        
        rate_limit_info = self.rate_limits[domain]
        
        if status_code == 429:
            # Rate limited - increase backoff
            rate_limit_info.consecutive_429s += 1
            rate_limit_info.last_429_time = datetime.now()
            
            if retry_after:
                rate_limit_info.retry_after = retry_after
            
            # Increase backoff multiplier for persistent rate limiting
            if rate_limit_info.consecutive_429s > 3:
                rate_limit_info.backoff_multiplier = min(rate_limit_info.backoff_multiplier * 1.5, 10.0)
            
            logger.warning(f"Rate limited by {domain}: consecutive_429s={rate_limit_info.consecutive_429s}, "
                         f"retry_after={retry_after}, backoff_multiplier={rate_limit_info.backoff_multiplier}")
        else:
            # Successful request - gradually reduce backoff
            if rate_limit_info.consecutive_429s > 0:
                rate_limit_info.consecutive_429s = max(0, rate_limit_info.consecutive_429s - 1)
            
            # Reset backoff multiplier after successful requests
            if rate_limit_info.consecutive_429s == 0:
                rate_limit_info.backoff_multiplier = max(1.0, rate_limit_info.backoff_multiplier * 0.9)
                rate_limit_info.retry_after = None
    
    async def should_delay(self, url: str) -> Tuple[bool, float]:
        """
        Check if we should delay before making a request
        Returns (should_delay, delay_seconds)
        """
        domain = self._get_domain_from_url(url)
        
        # First check if we can make a request within the 30/hour limit
        can_make_request, hourly_delay = self._can_make_request(domain)
        if not can_make_request:
            logger.info(f"Rate limited by hourly limit for {domain}: {hourly_delay:.1f}s delay")
            return True, hourly_delay
        
        if domain not in self.rate_limits:
            return False, 0.0
        
        rate_limit_info = self.rate_limits[domain]
        
        # Check if we're still in a retry-after period
        if rate_limit_info.retry_after and rate_limit_info.last_429_time:
            time_since_429 = datetime.now() - rate_limit_info.last_429_time
            if time_since_429.total_seconds() < rate_limit_info.retry_after:
                remaining_time = rate_limit_info.retry_after - time_since_429.total_seconds()
                return True, remaining_time
        
        # Check if we need exponential backoff
        if rate_limit_info.consecutive_429s > 0:
            delay = self._calculate_backoff_delay(rate_limit_info)
            return True, delay
        
        return False, 0.0
    
    async def handle_response(self, url: str, status_code: int, headers: Dict[str, str]):
        """Handle HTTP response and update rate limiting information"""
        domain = self._get_domain_from_url(url)
        
        # Extract retry-after header if present
        retry_after = None
        if 'retry-after' in headers:
            try:
                retry_after = int(headers['retry-after'])
            except ValueError:
                pass
        
        self._update_rate_limit_info(domain, status_code, retry_after)
    
    async def get_delay_for_domain(self, domain: str) -> float:
        """Get current delay recommendation for a domain"""
        if domain not in self.rate_limits:
            return 0.0
        
        rate_limit_info = self.rate_limits[domain]
        
        if rate_limit_info.consecutive_429s > 0:
            return self._calculate_backoff_delay(rate_limit_info)
        
        return 0.0
    
    def get_rate_limit_stats(self) -> Dict:
        """Get current rate limiting statistics"""
        stats = {
            'total_domains': len(self.rate_limits),
            'domains_with_backoff': 0,
            'total_consecutive_429s': 0,
            'domains': {}
        }
        
        for domain, info in self.rate_limits.items():
            if info.consecutive_429s > 0:
                stats['domains_with_backoff'] += 1
                stats['total_consecutive_429s'] += info.consecutive_429s
            
            stats['domains'][domain] = {
                'consecutive_429s': info.consecutive_429s,
                'backoff_multiplier': info.backoff_multiplier,
                'retry_after': info.retry_after,
                'last_429_time': info.last_429_time.isoformat() if info.last_429_time else None
            }
        
        return stats

# Global rate limiter instance with 30 requests per hour
rate_limiter = ExponentialBackoffRateLimiter(requests_per_hour=30)

async def make_request_with_backoff(session, url: str, **kwargs) -> Tuple[any, Dict[str, str]]:
    """
    Make HTTP request with exponential backoff for rate limiting
    
    Args:
        session: aiohttp ClientSession
        url: URL to request
        **kwargs: Additional arguments for session.get()
    
    Returns:
        Tuple of (response, headers_dict)
    """
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        # Check if we should delay before making the request
        should_delay, delay_seconds = await rate_limiter.should_delay(url)
        
        if should_delay:
            logger.info(f"Delaying request to {url} for {delay_seconds:.2f} seconds due to rate limiting")
            await asyncio.sleep(delay_seconds)
        
        try:
            # Record the request attempt
            domain = rate_limiter._get_domain_from_url(url)
            rate_limiter._record_request(domain)
            
            # Make the request
            async with session.get(url, **kwargs) as response:
                headers_dict = dict(response.headers)
                
                # Handle the response for rate limiting
                await rate_limiter.handle_response(url, response.status, headers_dict)
                
                # If we get a 429, we'll retry with backoff
                if response.status == 429:
                    retry_count += 1
                    if retry_count < max_retries:
                        # Calculate delay for retry
                        domain = rate_limiter._get_domain_from_url(url)
                        delay = await rate_limiter.get_delay_for_domain(domain)
                        logger.warning(f"Got 429 for {url}, retrying in {delay:.2f} seconds (attempt {retry_count + 1}/{max_retries})")
                        await asyncio.sleep(delay)
                        continue
                    else:
                        logger.error(f"Max retries exceeded for {url} due to rate limiting")
                        return response, headers_dict
                
                # Success or other error - return the response
                return response, headers_dict
                
        except Exception as e:
            logger.error(f"Error making request to {url}: {e}")
            retry_count += 1
            if retry_count < max_retries:
                # Exponential backoff for other errors too
                delay = min(2 ** retry_count, 30)  # Max 30 seconds
                await asyncio.sleep(delay)
            else:
                raise
    
    # This should never be reached, but just in case
    raise Exception(f"Failed to make request to {url} after {max_retries} retries")
