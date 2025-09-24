"""
Memory Cache Implementation for Performance Optimization
"""

import time
import threading
from collections import OrderedDict
from datetime import datetime, timedelta

class MemoryCache:
    def __init__(self, max_size=1000, default_ttl=3600):
        """Initialize memory cache with LRU eviction"""
        self.max_size = max_size
        self.default_ttl = default_ttl
        self.cache = OrderedDict()
        self.timestamps = {}
        self.lock = threading.RLock()
    
    def get(self, key):
        """Get value from cache"""
        with self.lock:
            if key not in self.cache:
                return None
            
            # Check if expired
            if time.time() > self.timestamps.get(key, 0):
                self._remove_key(key)
                return None
            
            # Move to end (most recently used)
            value = self.cache.pop(key)
            self.cache[key] = value
            return value
    
    def set(self, key, value, expire_seconds=None):
        """Set value in cache with expiration"""
        with self.lock:
            if expire_seconds is None:
                expire_seconds = self.default_ttl
            
            # Remove if exists
            if key in self.cache:
                self._remove_key(key)
            
            # Add new entry
            self.cache[key] = value
            self.timestamps[key] = time.time() + expire_seconds
            
            # Evict if over limit
            if len(self.cache) > self.max_size:
                self._evict_lru()
    
    def delete(self, key):
        """Delete key from cache"""
        with self.lock:
            return self._remove_key(key)
    
    def exists(self, key):
        """Check if key exists in cache"""
        with self.lock:
            if key not in self.cache:
                return False
            
            # Check if expired
            if time.time() > self.timestamps.get(key, 0):
                self._remove_key(key)
                return False
            
            return True
    
    def clear_pattern(self, pattern):
        """Clear all keys matching pattern (simple string matching)"""
        with self.lock:
            keys_to_remove = []
            for key in self.cache.keys():
                if pattern.replace('*', '') in key:
                    keys_to_remove.append(key)
            
            for key in keys_to_remove:
                self._remove_key(key)
            
            return len(keys_to_remove)
    
    def _remove_key(self, key):
        """Remove key from cache"""
        if key in self.cache:
            del self.cache[key]
            del self.timestamps[key]
            return True
        return False
    
    def _evict_lru(self):
        """Evict least recently used item"""
        if self.cache:
            # Remove first item (least recently used)
            key, _ = self.cache.popitem(last=False)
            if key in self.timestamps:
                del self.timestamps[key]
    
    def stats(self):
        """Get cache statistics"""
        with self.lock:
            now = time.time()
            expired_count = sum(1 for ts in self.timestamps.values() if now > ts)
            
            return {
                'size': len(self.cache),
                'max_size': self.max_size,
                'expired_entries': expired_count,
                'hit_ratio': getattr(self, '_hit_ratio', 0)
            }
    
    def cleanup_expired(self):
        """Remove expired entries"""
        with self.lock:
            now = time.time()
            expired_keys = [
                key for key, ts in self.timestamps.items() 
                if now > ts
            ]
            
            for key in expired_keys:
                self._remove_key(key)
            
            return len(expired_keys)

# Global memory cache instance
memory_cache = MemoryCache(max_size=2000, default_ttl=1800)

def cached(expire_seconds=1800, key_prefix="", use_args=True):
    """Decorator for caching function results in memory"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Generate cache key
            if use_args:
                key_data = f"{key_prefix}:{func.__name__}:{str(args)}:{str(sorted(kwargs.items()))}"
            else:
                key_data = f"{key_prefix}:{func.__name__}"
            
            cache_key = f"mem:{key_data}"
            
            # Try to get from cache
            cached_result = memory_cache.get(cache_key)
            if cached_result is not None:
                return cached_result
            
            # Execute function and cache result
            result = func(*args, **kwargs)
            memory_cache.set(cache_key, result, expire_seconds)
            return result
        
        return wrapper
    return decorator

def invalidate_cache(pattern):
    """Invalidate cache entries matching pattern"""
    memory_cache.clear_pattern(f"mem:{pattern}")

# Cache key patterns
CACHE_PATTERNS = {
    'stats': 'stats:*',
    'datasets': 'datasets:*',
    'metadata': 'metadata:*',
    'search': 'search:*',
    'wayback': 'wayback:*',
    'analytics': 'analytics:*'
}

