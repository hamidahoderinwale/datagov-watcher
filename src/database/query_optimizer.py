"""
Database Query Optimizer for Performance Enhancement
"""

import sqlite3
import time
from functools import wraps
from .connection import get_db_connection
from ..cache.cache_manager import cached, invalidate_cache, CACHE_PATTERNS

class QueryOptimizer:
    def __init__(self):
        self.query_stats = {
            'total_queries': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'avg_query_time': 0,
            'slow_queries': []
        }
        self.slow_query_threshold = 1.0  # seconds
    
    def optimize_query(self, query, params=None, cache_key=None, cache_ttl=3600):
        """Execute optimized query with caching"""
        start_time = time.time()
        
        # Try cache first if cache_key provided
        if cache_key:
            cached_result = self._get_cached_result(cache_key)
            if cached_result is not None:
                self.query_stats['cache_hits'] += 1
                return cached_result
        
        # Execute query
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # Use prepared statement for better performance
            cursor.execute(query, params or ())
            result = cursor.fetchall()
            
            conn.close()
            
            # Cache result if cache_key provided
            if cache_key:
                self._cache_result(cache_key, result, cache_ttl)
                self.query_stats['cache_misses'] += 1
            
            # Update stats
            query_time = time.time() - start_time
            self._update_query_stats(query, query_time)
            
            return result
            
        except Exception as e:
            print(f"Query execution error: {e}")
            raise
    
    def _get_cached_result(self, cache_key):
        """Get cached query result"""
        try:
            from ..cache.cache_manager import cache_manager
            return cache_manager.get(f"query:{cache_key}")
        except Exception:
            return None
    
    def _cache_result(self, cache_key, result, ttl):
        """Cache query result"""
        try:
            from ..cache.cache_manager import cache_manager
            cache_manager.set(f"query:{cache_key}", result, ttl)
        except Exception as e:
            print(f"Cache error: {e}")
    
    def _update_query_stats(self, query, query_time):
        """Update query performance statistics"""
        self.query_stats['total_queries'] += 1
        
        # Update average query time
        total_queries = self.query_stats['total_queries']
        current_avg = self.query_stats['avg_query_time']
        self.query_stats['avg_query_time'] = (
            (current_avg * (total_queries - 1) + query_time) / total_queries
        )
        
        # Track slow queries
        if query_time > self.slow_query_threshold:
            self.query_stats['slow_queries'].append({
                'query': query[:100] + '...' if len(query) > 100 else query,
                'time': query_time,
                'timestamp': time.time()
            })
            
            # Keep only last 50 slow queries
            if len(self.query_stats['slow_queries']) > 50:
                self.query_stats['slow_queries'] = self.query_stats['slow_queries'][-50:]
    
    def get_stats(self):
        """Get query performance statistics"""
        return self.query_stats.copy()
    
    def clear_stats(self):
        """Clear query statistics"""
        self.query_stats = {
            'total_queries': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'avg_query_time': 0,
            'slow_queries': []
        }

# Global query optimizer instance
query_optimizer = QueryOptimizer()

def optimized_query(cache_key=None, cache_ttl=3600):
    """Decorator for optimized database queries"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Generate cache key if not provided
            if cache_key is None:
                func_cache_key = f"{func.__name__}:{str(args)}:{str(sorted(kwargs.items()))}"
            else:
                func_cache_key = cache_key
            
            # Try cache first
            if func_cache_key:
                cached_result = query_optimizer._get_cached_result(func_cache_key)
                if cached_result is not None:
                    query_optimizer.query_stats['cache_hits'] += 1
                    return cached_result
            
            # Execute function
            start_time = time.time()
            result = func(*args, **kwargs)
            query_time = time.time() - start_time
            
            # Cache result
            if func_cache_key:
                query_optimizer._cache_result(func_cache_key, result, cache_ttl)
                query_optimizer.query_stats['cache_misses'] += 1
            
            # Update stats
            query_optimizer._update_query_stats(func.__name__, query_time)
            
            return result
        
        return wrapper
    return decorator

# Common optimized queries
class OptimizedQueries:
    @staticmethod
    @cached(key_prefix="stats", expire_seconds=300)
    def get_system_stats():
        """Get system statistics with caching"""
        query = """
            SELECT 
                COUNT(DISTINCT d.id) as total_datasets,
                COUNT(ds.id) as total_snapshots,
                COUNT(CASE WHEN ds.availability = 'available' THEN 1 END) as available_count,
                COUNT(CASE WHEN ds.availability = 'unavailable' THEN 1 END) as unavailable_count,
                COUNT(DISTINCT d.agency) as agencies_count
            FROM datasets d
            LEFT JOIN dataset_states ds ON d.id = ds.dataset_id
            WHERE ds.snapshot_date = (
                SELECT MAX(snapshot_date) 
                FROM dataset_states ds2 
                WHERE ds2.dataset_id = d.id
            )
        """
        return query_optimizer.optimize_query(query, cache_key="system_stats", cache_ttl=300)
    
    @staticmethod
    @cached(key_prefix="datasets", expire_seconds=600)
    def get_datasets_list(limit=50, offset=0, status=None):
        """Get datasets list with caching"""
        base_query = """
            SELECT 
                d.id,
                d.title,
                d.agency,
                d.url,
                d.description,
                ds.availability,
                ds.row_count,
                ds.column_count,
                ds.snapshot_date
            FROM datasets d
            LEFT JOIN dataset_states ds ON d.id = ds.dataset_id
            WHERE ds.snapshot_date = (
                SELECT MAX(snapshot_date) 
                FROM dataset_states ds2 
                WHERE ds2.dataset_id = d.id
            )
        """
        
        params = []
        if status:
            base_query += " AND ds.availability = ?"
            params.append(status)
        
        base_query += " ORDER BY d.title LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        
        cache_key = f"datasets_list:{limit}:{offset}:{status}"
        return query_optimizer.optimize_query(base_query, params, cache_key, 600)
    
    @staticmethod
    @cached(key_prefix="metadata", expire_seconds=1800)
    def get_dataset_metadata(dataset_id):
        """Get dataset metadata with caching"""
        query = """
            SELECT 
                d.id, d.title, d.agency, d.description, d.url, d.source,
                d.created_at, d.last_modified,
                ds.availability, ds.row_count, ds.column_count, ds.file_size,
                ds.resource_format, ds.schema_columns, ds.schema_dtypes,
                ds.snapshot_date
            FROM datasets d
            LEFT JOIN dataset_states ds ON d.id = ds.dataset_id
            WHERE d.id = ?
            ORDER BY ds.snapshot_date DESC
            LIMIT 1
        """
        cache_key = f"dataset_metadata:{dataset_id}"
        return query_optimizer.optimize_query(query, (dataset_id,), cache_key, 1800)
    
    @staticmethod
    @cached(key_prefix="wayback", expire_seconds=900)
    def get_dataset_timeline(dataset_id):
        """Get dataset timeline with caching"""
        query = """
            SELECT 
                snapshot_date,
                availability,
                row_count,
                column_count,
                file_size,
                content_hash,
                title,
                agency,
                url,
                resource_format,
                schema_columns,
                schema_dtypes
            FROM dataset_states 
            WHERE dataset_id = ? 
            ORDER BY snapshot_date DESC
        """
        cache_key = f"dataset_timeline:{dataset_id}"
        return query_optimizer.optimize_query(query, (dataset_id,), cache_key, 900)
    
    @staticmethod
    @cached(key_prefix="search", expire_seconds=300)
    def search_datasets(query_text, limit=50, offset=0):
        """Search datasets with caching"""
        query = """
            SELECT 
                d.id, 
                d.title, 
                d.agency, 
                d.description, 
                d.url, 
                ds.availability, 
                ds.row_count, 
                ds.column_count,
                ds.snapshot_date
            FROM datasets d
            LEFT JOIN dataset_states ds ON d.id = ds.dataset_id
            WHERE (d.title LIKE ? OR d.description LIKE ? OR d.agency LIKE ?)
            AND ds.snapshot_date = (
                SELECT MAX(snapshot_date) 
                FROM dataset_states ds2 
                WHERE ds2.dataset_id = d.id
            )
            GROUP BY d.id
            ORDER BY d.title
            LIMIT ? OFFSET ?
        """
        
        search_term = f'%{query_text}%'
        params = [search_term, search_term, search_term, limit, offset]
        cache_key = f"search_datasets:{query_text}:{limit}:{offset}"
        return query_optimizer.optimize_query(query, params, cache_key, 300)
    
    @staticmethod
    @cached(key_prefix="analytics", expire_seconds=600)
    def get_analytics_data(time_period=30):
        """Get analytics data with caching"""
        query = """
            SELECT 
                DATE(snapshot_date) as date,
                COUNT(DISTINCT dataset_id) as total_datasets,
                COUNT(CASE WHEN availability = 'available' THEN 1 END) as available_datasets,
                COUNT(CASE WHEN availability = 'unavailable' THEN 1 END) as unavailable_datasets
            FROM dataset_states 
            WHERE snapshot_date >= date('now', '-{} days')
            GROUP BY DATE(snapshot_date)
            ORDER BY date
        """.format(time_period)
        
        cache_key = f"analytics_data:{time_period}"
        return query_optimizer.optimize_query(query, cache_key=cache_key, cache_ttl=600)

# Database connection optimization
def get_optimized_connection():
    """Get optimized database connection with connection pooling"""
    return get_db_connection()

def close_connection(conn):
    """Close database connection"""
    if conn:
        conn.close()

