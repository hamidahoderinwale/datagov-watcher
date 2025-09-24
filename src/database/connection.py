"""
Database Connection Management
"""

import sqlite3
import threading
from contextlib import contextmanager

class DatabaseConnectionPool:
    def __init__(self, database_path, max_connections=10):
        self.database_path = database_path
        self.max_connections = max_connections
        self.connections = []
        self.lock = threading.Lock()
    
    def get_connection(self):
        """Get a database connection from the pool"""
        with self.lock:
            if self.connections:
                return self.connections.pop()
            else:
                return self._create_connection()
    
    def return_connection(self, conn):
        """Return a connection to the pool"""
        if conn and len(self.connections) < self.max_connections:
            with self.lock:
                self.connections.append(conn)
        else:
            if conn:
                conn.close()
    
    def _create_connection(self):
        """Create a new database connection"""
        conn = sqlite3.connect(
            self.database_path,
            check_same_thread=False,
            timeout=30.0
        )
        
        # Optimize connection settings
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=10000")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA mmap_size=268435456")  # 256MB
        
        return conn
    
    def close_all(self):
        """Close all connections in the pool"""
        with self.lock:
            for conn in self.connections:
                conn.close()
            self.connections.clear()

# Global connection pool
connection_pool = DatabaseConnectionPool('datasets.db', max_connections=20)

def get_db_connection():
    """Get a database connection"""
    return connection_pool.get_connection()

@contextmanager
def get_db_cursor():
    """Context manager for database cursor"""
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        yield cursor
    except Exception as e:
        if conn:
            conn.rollback()
        raise e
    finally:
        if conn:
            connection_pool.return_connection(conn)

def close_db_connection(conn):
    """Return a database connection to the pool"""
    if conn:
        connection_pool.return_connection(conn)

def init_database():
    """Initialize database with optimized settings"""
    conn = get_db_connection()
    try:
        # Create indexes for better performance
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_dataset_states_dataset_id ON dataset_states(dataset_id)",
            "CREATE INDEX IF NOT EXISTS idx_dataset_states_date ON dataset_states(snapshot_date)",
            "CREATE INDEX IF NOT EXISTS idx_dataset_states_availability ON dataset_states(availability)",
            "CREATE INDEX IF NOT EXISTS idx_datasets_agency ON datasets(agency)",
            "CREATE INDEX IF NOT EXISTS idx_datasets_title ON datasets(title)",
            "CREATE INDEX IF NOT EXISTS idx_state_diffs_dataset_id ON state_diffs(dataset_id)",
            "CREATE INDEX IF NOT EXISTS idx_state_diffs_date ON state_diffs(change_date)"
        ]
        
        for index_sql in indexes:
            conn.execute(index_sql)
        
        conn.commit()
        print("Database indexes created successfully")
        
    except Exception as e:
        print(f"Error initializing database: {e}")
        conn.rollback()
    finally:
        close_db_connection(conn)

def optimize_database():
    """Optimize database performance"""
    conn = get_db_connection()
    try:
        # Analyze tables for query optimization
        conn.execute("ANALYZE")
        
        # Vacuum to reclaim space
        conn.execute("VACUUM")
        
        conn.commit()
        print("Database optimization completed")
        
    except Exception as e:
        print(f"Error optimizing database: {e}")
    finally:
        close_db_connection(conn)

