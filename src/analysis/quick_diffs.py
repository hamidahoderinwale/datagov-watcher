#!/usr/bin/env python3
"""
Quick diff generation to populate dashboard
"""

import sqlite3
import json
from core.historian_core import DatasetStateHistorian

def generate_quick_diffs():
    print("Starting Quick diff generation for dashboard...")
    
    historian = DatasetStateHistorian("datasets.db")
    
    # Get a few datasets with multiple snapshots
    conn = sqlite3.connect("datasets.db")
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT dataset_id, COUNT(*) as snapshot_count
        FROM historian_snapshots 
        GROUP BY dataset_id 
        HAVING COUNT(*) > 1
        ORDER BY COUNT(*) DESC
        LIMIT 5
    """)
    
    datasets = cursor.fetchall()
    print(f"Processing {len(datasets)} datasets...")
    
    diffs_generated = 0
    
    for dataset_id, snapshot_count in datasets:
        print(f"Processing {dataset_id}...")
        
        try:
            snapshots = historian.get_snapshots(dataset_id)
            if len(snapshots) < 2:
                continue
            
            # Generate diff between first two snapshots
            from_snapshot = snapshots[0]
            to_snapshot = snapshots[1]
            
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
            print(f"  Success Generated diff (volatility: {diff.volatility_score:.3f})")
            
        except Exception as e:
            print(f"  Error Error: {e}")
            continue
    
    conn.commit()
    conn.close()
    
    print(f"\n Generated {diffs_generated} diffs!")
    
    # Show final stats
    conn = sqlite3.connect("datasets.db")
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM historian_diffs")
    total_diffs = cursor.fetchone()[0]
    print(f"Total diffs in database: {total_diffs}")
    
    cursor.execute("""
        SELECT dataset_id, volatility_score
        FROM historian_diffs
        ORDER BY volatility_score DESC
        LIMIT 5
    """)
    
    top_volatile = cursor.fetchall()
    print(f"\nTop volatile datasets:")
    for dataset_id, volatility in top_volatile:
        print(f"  - {dataset_id}: {volatility:.3f}")
    
    conn.close()

if __name__ == "__main__":
    generate_quick_diffs()


