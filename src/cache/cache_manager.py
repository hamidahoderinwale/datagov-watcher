"""
Unified Cache Manager - Redis with Memory Fallback
"""

from .redis_cache import cache as redis_cache, CACHE_PATTERNS as REDIS_PATTERNS
from .memory_cache import memory_cache, CACHE_PATTERNS as MEMORY_PATTERNS, cached as memory_cached, invalidate_cache as memory_invalidate
import time
import threading

class UnifiedCacheManager:
    def __init__(self):
        self.redis_available = redis_cache.connected
        self.cache_stats = {
            'redis_hits': 0,
            'memory_hits': 0,
            'misses': 0,
            'redis_errors': 0,
            'memory_errors': 0
        }
        self.stats_lock = threading.Lock()
    
    def get(self, key):
        """Get value from cache (Redis first, then memory)"""
        # Try Redis first if available
        if self.redis_available:
            try:
                result = redis_cache.get(key)
                if result is not None:
                    with self.stats_lock:
                        self.cache_stats['redis_hits'] += 1
                    return result
            except Exception as e:
                with self.stats_lock:
                    self.cache_stats['redis_errors'] += 1
                print(f"Redis cache error: {e}")
        
        # Fallback to memory cache
        try:
            result = memory_cache.get(key)
            if result is not None:
                with self.stats_lock:
                    self.cache_stats['memory_hits'] += 1
                return result
        except Exception as e:
            with self.stats_lock:
                self.cache_stats['memory_errors'] += 1
            print(f"Memory cache error: {e}")
        
        with self.stats_lock:
            self.cache_stats['misses'] += 1
        return None
    
    def set(self, key, value, expire_seconds=3600):
        """Set value in cache (both Redis and memory)"""
        success = False
        
        # Set in Redis if available
        if self.redis_available:
            try:
                redis_cache.set(key, value, expire_seconds)
                success = True
            except Exception as e:
                print(f"Redis cache set error: {e}")
        
        # Always set in memory cache as backup
        try:
            memory_cache.set(key, value, expire_seconds)
            success = True
        except Exception as e:
            print(f"Memory cache set error: {e}")
        
        return success
    
    def delete(self, key):
        """Delete key from both caches"""
        redis_success = True
        memory_success = True
        
        if self.redis_available:
            try:
                redis_success = redis_cache.delete(key)
            except Exception as e:
                print(f"Redis cache delete error: {e}")
                redis_success = False
        
        try:
            memory_success = memory_cache.delete(key)
        except Exception as e:
            print(f"Memory cache delete error: {e}")
            memory_success = False
        
        return redis_success or memory_success
    
    def exists(self, key):
        """Check if key exists in either cache"""
        if self.redis_available:
            try:
                if redis_cache.exists(key):
                    return True
            except Exception as e:
                print(f"Redis cache exists error: {e}")
        
        try:
            return memory_cache.exists(key)
        except Exception as e:
            print(f"Memory cache exists error: {e}")
            return False
    
    def clear_pattern(self, pattern):
        """Clear all keys matching pattern from both caches"""
        redis_count = 0
        memory_count = 0
        
        if self.redis_available:
            try:
                redis_count = redis_cache.clear_pattern(pattern)
            except Exception as e:
                print(f"Redis cache clear pattern error: {e}")
        
        try:
            memory_count = memory_cache.clear_pattern(pattern)
        except Exception as e:
            print(f"Memory cache clear pattern error: {e}")
        
        return redis_count + memory_count
    
    def get_stats(self):
        """Get comprehensive cache statistics"""
        with self.stats_lock:
            stats = self.cache_stats.copy()
        
        # Add Redis stats if available
        if self.redis_available:
            try:
                redis_info = redis_cache.redis_client.info()
                stats.update({
                    'redis_connected': True,
                    'redis_memory_used': redis_info.get('used_memory_human', 'N/A'),
                    'redis_keys': redis_info.get('db0', {}).get('keys', 0)
                })
            except Exception as e:
                stats.update({
                    'redis_connected': False,
                    'redis_error': str(e)
                })
        else:
            stats['redis_connected'] = False
        
        # Add memory cache stats
        try:
            memory_stats = memory_cache.stats()
            stats.update({
                'memory_cache_size': memory_stats['size'],
                'memory_cache_max_size': memory_stats['max_size'],
                'memory_expired_entries': memory_stats['expired_entries']
            })
        except Exception as e:
            stats['memory_error'] = str(e)
        
        # Calculate hit ratio
        total_requests = stats['redis_hits'] + stats['memory_hits'] + stats['misses']
        if total_requests > 0:
            stats['hit_ratio'] = (stats['redis_hits'] + stats['memory_hits']) / total_requests
        else:
            stats['hit_ratio'] = 0
        
        return stats
    
    def cleanup(self):
        """Cleanup expired entries from memory cache"""
        try:
            return memory_cache.cleanup_expired()
        except Exception as e:
            print(f"Memory cache cleanup error: {e}")
            return 0

# Global cache manager instance
cache_manager = UnifiedCacheManager()

def cached(expire_seconds=3600, key_prefix="", use_args=True):
    """Decorator for caching function results"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            # Generate cache key
            if use_args:
                key_data = f"{key_prefix}:{func.__name__}:{str(args)}:{str(sorted(kwargs.items()))}"
            else:
                key_data = f"{key_prefix}:{func.__name__}"
            
            cache_key = f"unified:{key_data}"
            
            # Try to get from cache
            cached_result = cache_manager.get(cache_key)
            if cached_result is not None:
                return cached_result
            
            # Execute function and cache result
            result = func(*args, **kwargs)
            cache_manager.set(cache_key, result, expire_seconds)
            return result
        
        return wrapper
    return decorator

def invalidate_cache(pattern):
    """Invalidate cache entries matching pattern"""
    cache_manager.clear_pattern(f"unified:{pattern}")

# Cache key patterns
CACHE_PATTERNS = {
    'stats': 'stats:*',
    'datasets': 'datasets:*',
    'metadata': 'metadata:*',
    'search': 'search:*',
    'wayback': 'wayback:*',
    'analytics': 'analytics:*'
}

# Background cleanup thread
def start_cache_cleanup():
    """Start background cache cleanup thread"""
    def cleanup_worker():
        while True:
            try:
                time.sleep(300)  # Cleanup every 5 minutes
                cache_manager.cleanup()
            except Exception as e:
                print(f"Cache cleanup error: {e}")
    
    import threading
    cleanup_thread = threading.Thread(target=cleanup_worker, daemon=True)
    cleanup_thread.start()
    return cleanup_thread

