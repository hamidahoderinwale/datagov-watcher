"""
Redis Cache Implementation for Performance Optimization
"""

import redis
import json
import pickle
from datetime import datetime, timedelta
from functools import wraps
import hashlib

class RedisCache:
    def __init__(self, host='localhost', port=6379, db=0, password=None):
        """Initialize Redis cache connection"""
        try:
            self.redis_client = redis.Redis(
                host=host, 
                port=port, 
                db=db, 
                password=password,
                decode_responses=False  # We'll handle encoding ourselves
            )
            # Test connection
            self.redis_client.ping()
            self.connected = True
        except Exception as e:
            print(f"Redis connection failed: {e}")
            self.redis_client = None
            self.connected = False
    
    def get(self, key):
        """Get value from cache"""
        if not self.connected:
            return None
        
        try:
            value = self.redis_client.get(key)
            if value:
                return pickle.loads(value)
            return None
        except Exception as e:
            print(f"Cache get error: {e}")
            return None
    
    def set(self, key, value, expire_seconds=3600):
        """Set value in cache with expiration"""
        if not self.connected:
            return False
        
        try:
            serialized_value = pickle.dumps(value)
            return self.redis_client.setex(key, expire_seconds, serialized_value)
        except Exception as e:
            print(f"Cache set error: {e}")
            return False
    
    def delete(self, key):
        """Delete key from cache"""
        if not self.connected:
            return False
        
        try:
            return self.redis_client.delete(key)
        except Exception as e:
            print(f"Cache delete error: {e}")
            return False
    
    def exists(self, key):
        """Check if key exists in cache"""
        if not self.connected:
            return False
        
        try:
            return self.redis_client.exists(key)
        except Exception as e:
            print(f"Cache exists error: {e}")
            return False
    
    def clear_pattern(self, pattern):
        """Clear all keys matching pattern"""
        if not self.connected:
            return False
        
        try:
            keys = self.redis_client.keys(pattern)
            if keys:
                return self.redis_client.delete(*keys)
            return True
        except Exception as e:
            print(f"Cache clear pattern error: {e}")
            return False

# Global cache instance
cache = RedisCache()

def cached(expire_seconds=3600, key_prefix="", use_args=True):
    """Decorator for caching function results"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if not cache.connected:
                return func(*args, **kwargs)
            
            # Generate cache key
            if use_args:
                key_data = f"{key_prefix}:{func.__name__}:{str(args)}:{str(sorted(kwargs.items()))}"
            else:
                key_data = f"{key_prefix}:{func.__name__}"
            
            cache_key = hashlib.md5(key_data.encode()).hexdigest()
            
            # Try to get from cache
            cached_result = cache.get(cache_key)
            if cached_result is not None:
                return cached_result
            
            # Execute function and cache result
            result = func(*args, **kwargs)
            cache.set(cache_key, result, expire_seconds)
            return result
        
        return wrapper
    return decorator

def invalidate_cache(pattern):
    """Invalidate cache entries matching pattern"""
    if cache.connected:
        cache.clear_pattern(pattern)

# Cache key patterns
CACHE_PATTERNS = {
    'stats': 'stats:*',
    'datasets': 'datasets:*',
    'metadata': 'metadata:*',
    'search': 'search:*',
    'wayback': 'wayback:*',
    'analytics': 'analytics:*'
}

