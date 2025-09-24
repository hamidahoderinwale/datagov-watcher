#!/usr/bin/env python3
"""
Test script for the new 30 requests per hour rate limiting
"""

import asyncio
import aiohttp
import time
from src.monitoring.rate_limiter import rate_limiter, make_request_with_backoff

async def test_rate_limiting():
    """Test the 30 requests per hour rate limiting"""
    print("Testing 30 requests per hour rate limiting...")
    
    # Test URLs (using example.com as it's reliable)
    test_urls = [
        "https://httpbin.org/delay/1",
        "https://httpbin.org/status/200",
        "https://httpbin.org/json"
    ]
    
    async with aiohttp.ClientSession() as session:
        print(f"Making {len(test_urls)} requests to test rate limiting...")
        
        for i, url in enumerate(test_urls):
            print(f"Request {i+1}: {url}")
            
            # Check if we should delay
            should_delay, delay = await rate_limiter.should_delay(url)
            if should_delay:
                print(f"  Rate limited: delaying {delay:.1f} seconds")
                await asyncio.sleep(delay)
            
            # Make the request
            try:
                response, headers = await make_request_with_backoff(session, url)
                print(f"  Status: {response.status}")
            except Exception as e:
                print(f"  Error: {e}")
            
            # Small delay between requests
            await asyncio.sleep(1)
    
    # Show rate limiting stats
    stats = rate_limiter.get_rate_limit_stats()
    print(f"\nRate limiting stats:")
    print(f"  Total domains: {stats['total_domains']}")
    print(f"  Domains with backoff: {stats['domains_with_backoff']}")
    print(f"  Total consecutive 429s: {stats['total_consecutive_429s']}")
    
    # Test the 30/hour limit
    print(f"\nTesting 30/hour limit...")
    domain = "httpbin.org"
    
    # Simulate making 35 requests quickly
    for i in range(35):
        can_make, delay = rate_limiter._can_make_request(domain)
        if not can_make:
            print(f"Request {i+1}: Rate limited, delay {delay:.1f} seconds")
            break
        else:
            rate_limiter._record_request(domain)
            print(f"Request {i+1}: Allowed")
    
    # Show final stats
    stats = rate_limiter.get_rate_limit_stats()
    print(f"\nFinal stats:")
    print(f"  Total domains: {stats['total_domains']}")
    if domain in stats['domains']:
        domain_info = stats['domains'][domain]
        print(f"  {domain} consecutive 429s: {domain_info['consecutive_429s']}")

if __name__ == "__main__":
    asyncio.run(test_rate_limiting())