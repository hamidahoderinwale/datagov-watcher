#!/usr/bin/env python3
"""
Scheduler for automated weekly dataset monitoring checks
Runs every Monday at 9:00 AM PT
"""

import schedule
import time
import logging
from datetime import datetime
import sys
import os

# Add the current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.data_fetcher import DataFetcher
from analysis.diff_engine import DiffEngine
from integrations.wayback_client import WaybackClient
from core.report_generator import ReportGenerator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_weekly_check():
    """Run the weekly dataset monitoring check"""
    logger.info("Starting scheduled weekly dataset monitoring check...")
    
    try:
        # Initialize components
        data_fetcher = DataFetcher()
        diff_engine = DiffEngine()
        wayback_client = WaybackClient()
        report_generator = ReportGenerator()
        
        # Fetch data from both sources
        logger.info("Fetching LIL manifest...")
        lil_data = data_fetcher.fetch_lil_manifest()
        logger.info(f"Found {len(lil_data)} datasets in LIL archive")
        
        logger.info("Fetching live Data.gov catalog...")
        live_data = data_fetcher.fetch_live_datagov_catalog()
        logger.info(f"Found {len(live_data)} datasets in live catalog")
        
        # Find vanished datasets
        logger.info("Comparing datasets...")
        vanished = diff_engine.find_vanished_datasets()
        logger.info(f"Found {len(vanished)} vanished datasets")
        
        # Enrich with Wayback Machine data
        if vanished:
            logger.info("Enriching with Wayback Machine data...")
            enriched_vanished = []
            for i, dataset in enumerate(vanished, 1):
                logger.info(f"Processing dataset {i}/{len(vanished)}: {dataset['title']}")
                enriched = wayback_client.enrich_vanished_dataset(dataset)
                enriched_vanished.append(enriched)
        else:
            enriched_vanished = vanished
        
        # Generate reports
        logger.info("Generating reports...")
        csv_path = report_generator.generate_csv_report(enriched_vanished)
        html_path = report_generator.generate_html_report(enriched_vanished, diff_engine.get_comparison_stats())
        
        logger.info(f"CSV report: {csv_path}")
        logger.info(f"HTML report: {html_path}")
        
        # Log the check
        data_fetcher.log_monitoring_check('scheduled', len(live_data), len(enriched_vanished))
        
        # Summary
        logger.info("=" * 50)
        logger.info("WEEKLY CHECK SUMMARY")
        logger.info("=" * 50)
        logger.info(f"LIL Archive Datasets: {len(lil_data)}")
        logger.info(f"Live Datasets: {len(live_data)}")
        logger.info(f"Vanished Datasets: {len(enriched_vanished)}")
        
        if enriched_vanished:
            logger.info("\nVanished Datasets:")
            for dataset in enriched_vanished:
                logger.info(f"  - {dataset['title']} ({dataset['agency']})")
                logger.info(f"    Status: {dataset['status']}")
                logger.info(f"    Cause: {dataset['suspected_cause']}")
                logger.info(f"    Last Seen: {dataset['last_seen_date']}")
                logger.info("")
        
        logger.info("Weekly check completed successfully!")
        
    except Exception as e:
        logger.error(f"Error during weekly check: {e}")
        # Log the error but don't crash the scheduler
        try:
            data_fetcher = DataFetcher()
            data_fetcher.log_monitoring_check('scheduled_error', 0, 0)
        except:
            pass

def setup_schedule():
    """Set up the weekly schedule"""
    # Schedule for every Monday at 9:00 AM PT (Pacific Time)
    # Note: This assumes the system is running in PT timezone
    schedule.every().monday.at("09:00").do(run_weekly_check)
    
    logger.info("Scheduler configured for weekly checks on Mondays at 9:00 AM PT")
    logger.info("Next scheduled run: " + str(schedule.next_run()))

def run_scheduler():
    """Run the scheduler continuously"""
    setup_schedule()
    
    logger.info("Starting dataset monitoring scheduler...")
    logger.info("Press Ctrl+C to stop")
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user")
    except Exception as e:
        logger.error(f"Scheduler error: {e}")

if __name__ == "__main__":
    run_scheduler()
