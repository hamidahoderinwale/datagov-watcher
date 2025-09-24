"""
Daily Scheduler: Automated daily monitoring and snapshot creation
Part of Phase 1: Time-Series Foundation
"""

import schedule
import time
import logging
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from src.analysis.time_series_manager import TimeSeriesManager
from src.monitoring.enhanced_monitor import EnhancedConcordanceMonitor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('daily_monitoring.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DailyScheduler:
    def __init__(self, db_path: str = "datasets.db"):
        self.db_path = db_path
        self.time_series_manager = TimeSeriesManager(db_path)
        self.monitor = EnhancedConcordanceMonitor(db_path)
    
    def run_daily_monitoring(self):
        """Run daily monitoring and create snapshot"""
        logger.info("Starting daily monitoring cycle...")
        
        try:
            # Run enhanced monitor (limited to prevent overwhelming the system)
            logger.info("Running enhanced monitor...")
            self.monitor.run_monitoring(max_datasets=5000)  # Limit to 5000 datasets per day
            
            # Create daily snapshot
            logger.info("Creating daily snapshot...")
            snapshot_data = self.time_series_manager.create_daily_snapshot()
            
            # Detect changes
            logger.info("Detecting changes...")
            changes = self.time_series_manager.detect_changes()
            
            # Log summary
            logger.info(f"Daily monitoring complete:")
            logger.info(f"  - Datasets: {snapshot_data['total_datasets']}")
            logger.info(f"  - Available: {snapshot_data['available_datasets']} ({snapshot_data['availability_rate']:.1f}%)")
            logger.info(f"  - Changes detected: {len(changes)}")
            
            # Log significant changes
            significant_changes = [c for c in changes if c['severity'] in ['warning', 'error']]
            if significant_changes:
                logger.warning(f"Significant changes detected: {len(significant_changes)}")
                for change in significant_changes[:5]:  # Log first 5
                    logger.warning(f"  - {change['change_description']}")
            
            return True
            
        except Exception as e:
            logger.error(f"Daily monitoring failed: {e}")
            return False
    
    def run_full_monitoring(self):
        """Run full monitoring (all datasets) - use sparingly"""
        logger.info("Starting FULL monitoring cycle...")
        
        try:
            # Run enhanced monitor on all datasets
            logger.info("Running enhanced monitor on ALL datasets...")
            self.monitor.run_monitoring()  # No limit - all datasets
            
            # Create daily snapshot
            logger.info("Creating daily snapshot...")
            snapshot_data = self.time_series_manager.create_daily_snapshot()
            
            # Detect changes
            logger.info("Detecting changes...")
            changes = self.time_series_manager.detect_changes()
            
            logger.info(f"FULL monitoring complete:")
            logger.info(f"  - Datasets: {snapshot_data['total_datasets']}")
            logger.info(f"  - Available: {snapshot_data['available_datasets']} ({snapshot_data['availability_rate']:.1f}%)")
            logger.info(f"  - Changes detected: {len(changes)}")
            
            return True
            
        except Exception as e:
            logger.error(f"FULL monitoring failed: {e}")
            return False
    
    def start_scheduler(self):
        """Start the daily scheduler"""
        logger.info("Starting daily scheduler...")
        
        # Schedule daily monitoring at 2 AM
        schedule.every().day.at("02:00").do(self.run_daily_monitoring)
        
        # Schedule weekly full monitoring on Sundays at 3 AM
        schedule.every().sunday.at("03:00").do(self.run_full_monitoring)
        
        # Schedule immediate run for testing (remove in production)
        schedule.every().minute.do(self.run_daily_monitoring)
        
        logger.info("Scheduler started. Daily monitoring at 2 AM, Full monitoring on Sundays at 3 AM")
        
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    
    def run_manual_snapshot(self):
        """Run a manual snapshot without full monitoring"""
        logger.info("Running manual snapshot...")
        
        try:
            # Create snapshot from current data
            snapshot_data = self.time_series_manager.create_daily_snapshot()
            
            # Detect changes
            changes = self.time_series_manager.detect_changes()
            
            logger.info(f"Manual snapshot complete:")
            logger.info(f"  - Datasets: {snapshot_data['total_datasets']}")
            logger.info(f"  - Available: {snapshot_data['available_datasets']} ({snapshot_data['availability_rate']:.1f}%)")
            logger.info(f"  - Changes detected: {len(changes)}")
            
            return snapshot_data, changes
            
        except Exception as e:
            logger.error(f"Manual snapshot failed: {e}")
            return None, []

def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Daily Monitoring Scheduler')
    parser.add_argument('--mode', choices=['scheduler', 'manual', 'full'], 
                       default='manual', help='Run mode')
    parser.add_argument('--db-path', default='datasets.db', help='Database path')
    
    args = parser.parse_args()
    
    scheduler = DailyScheduler(args.db_path)
    
    if args.mode == 'scheduler':
        scheduler.start_scheduler()
    elif args.mode == 'manual':
        snapshot_data, changes = scheduler.run_manual_snapshot()
        if snapshot_data:
            print(f"Manual snapshot created: {snapshot_data['total_datasets']} datasets, {len(changes)} changes")
        else:
            print("Manual snapshot failed")
    elif args.mode == 'full':
        success = scheduler.run_full_monitoring()
        if success:
            print("Full monitoring completed")
        else:
            print("Full monitoring failed")

if __name__ == "__main__":
    main()


