#!/usr/bin/env python3
"""
Update Database Schema for Enhanced Dimension Tracking
Adds columns and tables needed for comprehensive row/column tracking
"""

import sqlite3
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def update_database_schema(db_path: str = "datasets.db"):
    """Update database schema for enhanced dimension tracking"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        print("🔧 Updating database schema for enhanced dimension tracking...")
        
        # Check if dataset_states table exists and add dimension tracking columns
        cursor.execute("PRAGMA table_info(dataset_states)")
        columns = [row[1] for row in cursor.fetchall()]
        
        # Add dimension tracking columns if they don't exist
        dimension_columns = [
            ('dimensions_computed', 'BOOLEAN DEFAULT FALSE'),
            ('dimension_computation_date', 'TIMESTAMP'),
            ('dimension_computation_error', 'TEXT'),
            ('dimension_computation_time_ms', 'INTEGER'),
            ('schema_columns', 'TEXT'),  # JSON array of column names
            ('schema_dtypes', 'TEXT'),   # JSON object of column data types
            ('content_analyzed', 'BOOLEAN DEFAULT FALSE'),
            ('analysis_quality_score', 'REAL')
        ]
        
        for column_name, column_type in dimension_columns:
            if column_name not in columns:
                try:
                    cursor.execute(f'ALTER TABLE dataset_states ADD COLUMN {column_name} {column_type}')
                    print(f"✅ Added column: {column_name}")
                except sqlite3.OperationalError as e:
                    if "duplicate column name" not in str(e).lower():
                        print(f"⚠️  Could not add column {column_name}: {e}")
                    else:
                        print(f"ℹ️  Column {column_name} already exists")
        
        # Create dimension computation log table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dimension_computation_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id TEXT NOT NULL,
                computation_date TIMESTAMP NOT NULL,
                success BOOLEAN NOT NULL,
                row_count INTEGER,
                column_count INTEGER,
                file_size INTEGER,
                error_message TEXT,
                computation_time_ms INTEGER,
                resource_format TEXT,
                analysis_method TEXT,
                quality_score REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        print("✅ Created dimension_computation_log table")
        
        # Create dimension history table for tracking changes over time
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dimension_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id TEXT NOT NULL,
                snapshot_date TEXT NOT NULL,
                row_count INTEGER,
                column_count INTEGER,
                file_size INTEGER,
                schema_hash TEXT,
                change_detected BOOLEAN DEFAULT FALSE,
                change_type TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(dataset_id, snapshot_date)
            )
        ''')
        print("✅ Created dimension_history table")
        
        # Create dimension quality metrics table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dimension_quality_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id TEXT NOT NULL,
                metric_name TEXT NOT NULL,
                metric_value REAL NOT NULL,
                computation_date TIMESTAMP NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        print("✅ Created dimension_quality_metrics table")
        
        # Create indexes for better performance
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_dataset_states_dimensions ON dataset_states(dimensions_computed, availability)",
            "CREATE INDEX IF NOT EXISTS idx_dataset_states_dataset_id ON dataset_states(dataset_id)",
            "CREATE INDEX IF NOT EXISTS idx_dimension_log_dataset_id ON dimension_computation_log(dataset_id)",
            "CREATE INDEX IF NOT EXISTS idx_dimension_log_date ON dimension_computation_log(computation_date)",
            "CREATE INDEX IF NOT EXISTS idx_dimension_history_dataset_id ON dimension_history(dataset_id)",
            "CREATE INDEX IF NOT EXISTS idx_dimension_quality_dataset_id ON dimension_quality_metrics(dataset_id)"
        ]
        
        for index_sql in indexes:
            try:
                cursor.execute(index_sql)
                print(f"✅ Created index: {index_sql.split('idx_')[1].split(' ON')[0]}")
            except sqlite3.OperationalError as e:
                print(f"⚠️  Could not create index: {e}")
        
        # Update existing records to mark dimensions as computed where we have data
        cursor.execute('''
            UPDATE dataset_states 
            SET dimensions_computed = TRUE,
                dimension_computation_date = created_at
            WHERE row_count IS NOT NULL 
            AND row_count > 0 
            AND column_count IS NOT NULL 
            AND column_count > 0
        ''')
        updated_records = cursor.rowcount
        print(f"✅ Updated {updated_records} existing records to mark dimensions as computed")
        
        # Create a view for easy access to dimension statistics
        cursor.execute('''
            CREATE VIEW IF NOT EXISTS dimension_summary AS
            SELECT 
                ds.agency,
                COUNT(DISTINCT ds.dataset_id) as total_datasets,
                COUNT(CASE WHEN ds.dimensions_computed = TRUE THEN 1 END) as datasets_with_dimensions,
                COUNT(CASE WHEN ds.dimensions_computed = FALSE AND ds.availability = 'available' THEN 1 END) as datasets_missing_dimensions,
                AVG(CASE WHEN ds.dimensions_computed = TRUE THEN ds.row_count END) as avg_rows,
                AVG(CASE WHEN ds.dimensions_computed = TRUE THEN ds.column_count END) as avg_columns,
                MAX(ds.dimension_computation_date) as last_computation
            FROM dataset_states ds
            INNER JOIN (
                SELECT dataset_id, MAX(created_at) as max_created
                FROM dataset_states 
                GROUP BY dataset_id
            ) latest ON ds.dataset_id = latest.dataset_id 
            AND ds.created_at = latest.max_created
            GROUP BY ds.agency
            ORDER BY total_datasets DESC
        ''')
        print("✅ Created dimension_summary view")
        
        # Commit all changes
        conn.commit()
        print("\n🎉 Database schema update completed successfully!")
        
        # Show summary statistics
        cursor.execute("SELECT COUNT(*) FROM dataset_states WHERE dimensions_computed = TRUE")
        with_dimensions = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM dataset_states WHERE dimensions_computed = FALSE AND availability = 'available'")
        missing_dimensions = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT dataset_id) FROM dataset_states")
        total_datasets = cursor.fetchone()[0]
        
        print(f"\n📊 Current Status:")
        print(f"   Total datasets: {total_datasets}")
        print(f"   With dimensions: {with_dimensions}")
        print(f"   Missing dimensions: {missing_dimensions}")
        print(f"   Completion rate: {(with_dimensions / total_datasets * 100):.1f}%" if total_datasets > 0 else "   Completion rate: 0%")
        
    except Exception as e:
        print(f"❌ Error updating schema: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Update database schema for dimension tracking')
    parser.add_argument('--db', default='datasets.db', help='Database file path')
    
    args = parser.parse_args()
    
    print("🧬 Dataset State Historian - Schema Update")
    print(f"📊 Database: {args.db}")
    print(f"⏰ Started at: {datetime.now().isoformat()}")
    print("-" * 60)
    
    update_database_schema(args.db)
    
    print(f"\n⏰ Completed at: {datetime.now().isoformat()}")

if __name__ == '__main__':
    main()


