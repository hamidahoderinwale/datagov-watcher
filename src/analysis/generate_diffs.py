#!/usr/bin/env python3
"""
Generate diffs and volatility metrics for the dashboard
Simplified version without complex dependencies
"""

import sqlite3
import json
import logging
from datetime import datetime
from core.historian_core import DatasetStateHistorian

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def generate_diffs_and_volatility():
    """Generate diffs and volatility metrics for all datasets"""
    print(" Generating diffs and volatility metrics...")
    print("=" * 60)
    
    # Initialize historian
    historian = DatasetStateHistorian("datasets.db")
    
    # Get database connection
    conn = sqlite3.connect("datasets.db")
    cursor = conn.cursor()
    
    # Get datasets with multiple snapshots
    cursor.execute("""
        SELECT dataset_id, COUNT(*) as snapshot_count
        FROM historian_snapshots 
        GROUP BY dataset_id 
        HAVING COUNT(*) > 1
        ORDER BY COUNT(*) DESC
    """)
    
    datasets = cursor.fetchall()
    print(f"Found {len(datasets)} datasets with multiple snapshots")
    
    diffs_generated = 0
    volatility_metrics_generated = 0
    
    # Process datasets in batches
    for i, (dataset_id, snapshot_count) in enumerate(datasets):
        if i % 100 == 0:
            print(f"Processing dataset {i+1}/{len(datasets)}: {dataset_id}")
        
        try:
            # Get snapshots for this dataset
            snapshots = historian.get_snapshots(dataset_id)
            
            if len(snapshots) < 2:
                continue
                
            # Generate diffs between consecutive snapshots
            for j in range(1, len(snapshots)):
                from_snapshot = snapshots[j-1]
                to_snapshot = snapshots[j]
                
                # Check if diff already exists
                cursor.execute("""
                    SELECT id FROM historian_diffs
                    WHERE dataset_id = ? AND from_date = ? AND to_date = ?
                """, (dataset_id, from_snapshot.snapshot_date, to_snapshot.snapshot_date))
                
                if cursor.fetchone():
                    continue
                
                # Generate diff
                diff = historian.compute_diff(from_snapshot, to_snapshot)
                
                # Store diff
                cursor.execute("""
                    INSERT OR REPLACE INTO historian_diffs
                    (dataset_id, from_date, to_date, from_source, to_source,
                     metadata_changes, schema_changes, content_changes,
                     volatility_score, change_events)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    diff.dataset_id,
                    diff.from_date,
                    diff.to_date,
                    'live',
                    'live',
                    json.dumps(diff.metadata_changes),
                    json.dumps(diff.schema_changes),
                    json.dumps(diff.content_changes),
                    diff.volatility_score,
                    json.dumps(diff.change_events)
                ))
                
                diffs_generated += 1
                
                # Store volatility metrics
                cursor.execute("""
                    INSERT OR REPLACE INTO volatility_metrics
                    (dataset_id, snapshot_date, volatility_score, schema_churn_rate, 
                     content_similarity, license_changed, url_changed, publisher_changed,
                     row_count_delta, column_count_delta, metadata)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    dataset_id,
                    diff.to_date,
                    diff.volatility_score,
                    len(diff.schema_changes) / max(len(from_snapshot.schema), 1),
                    1.0 - diff.content_changes.get('content_drift', 0.0),
                    any('license' in str(change).lower() for change in diff.metadata_changes),
                    any('url' in str(change).lower() for change in diff.metadata_changes),
                    any('publisher' in str(change).lower() for change in diff.metadata_changes),
                    diff.content_changes.get('row_count_delta', 0),
                    diff.content_changes.get('column_count_delta', 0),
                    json.dumps(diff.change_events)
                ))
                
                volatility_metrics_generated += 1
                
        except Exception as e:
            logger.error(f"Error processing {dataset_id}: {e}")
            continue
    
    # Commit all changes
    conn.commit()
    conn.close()
    
    print(f"\nSuccess Generated {diffs_generated} diffs and {volatility_metrics_generated} volatility metrics")
    
    # Show final statistics
    conn = sqlite3.connect("datasets.db")
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM historian_diffs")
    total_diffs = cursor.fetchone()[0]
    print(f"Total diffs in database: {total_diffs:,}")
    
    cursor.execute("SELECT COUNT(*) FROM volatility_metrics")
    total_volatility = cursor.fetchone()[0]
    print(f"Total volatility metrics: {total_volatility:,}")
    
    # Show high volatility datasets
    cursor.execute("""
        SELECT dataset_id, AVG(volatility_score) as avg_volatility
        FROM volatility_metrics
        GROUP BY dataset_id
        HAVING avg_volatility > 0.5
        ORDER BY avg_volatility DESC
        LIMIT 10
    """)
    
    high_volatility = cursor.fetchall()
    print(f"\nHigh volatility datasets (>0.5): {len(high_volatility)}")
    
    if high_volatility:
        print("Top volatile datasets:")
        for dataset_id, volatility in high_volatility:
            print(f"  - {dataset_id}: {volatility:.3f}")
    
    conn.close()
    
    print("\n Dashboard data generation completed!")
    print("Your dashboard should now show:")
    print("  Success Change Events: Real detected changes")
    print("  Success High Risk Datasets: Datasets with volatility > 0.7")
    print("  Success Recent Changes: Actual recent changes")
    print("  Success Top Volatile Datasets: Real volatility rankings")

if __name__ == "__main__":
    generate_diffs_and_volatility()


