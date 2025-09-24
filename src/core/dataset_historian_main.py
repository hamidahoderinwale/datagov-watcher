"""
Dataset State Historian - Main Orchestrator
Integrates all components for comprehensive dataset state tracking
"""

import asyncio
import aiohttp
import sqlite3
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import logging
from pathlib import Path
import argparse
import schedule
import threading

from core.historian_core import DatasetStateHistorian, DatasetSnapshot
from integrations.lil_integration import LILIntegration
from integrations.wayback_enhanced import WaybackEnhanced
from integrations.wayback_fetcher import WaybackFetcher
from visualization.timeline_ui import TimelineUI
from monitoring.enhanced_monitor import EnhancedConcordanceMonitor

# Post-mortem system will be imported conditionally to avoid WeasyPrint issues
POSTMORTEM_AVAILABLE = False
PostMortemSystem = None

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('historian.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DatasetHistorianMain:
    """Main orchestrator for the Dataset State Historian system"""
    
    def __init__(self, db_path: str = "datasets.db", config_file: str = "historian_config.json"):
        self.db_path = db_path
        self.config_file = config_file
        self.config = self._load_config()
        
        # Initialize components
        self.historian = DatasetStateHistorian(db_path)
        self.lil = LILIntegration(db_path)
        self.wayback = WaybackEnhanced(db_path)
        self.wayback_fetcher = WaybackFetcher(db_path)
        self.timeline_ui = TimelineUI(db_path)
        self.enhanced_monitor = EnhancedConcordanceMonitor(db_path)
        
        # Try to import post-mortem system conditionally
        try:
            from analysis.postmortem_system import PostMortemSystem
            self.postmortem = PostMortemSystem(db_path)
            POSTMORTEM_AVAILABLE = True
        except (ImportError, OSError) as e:
            logger.warning(f"Post-mortem system not available: {e}")
            self.postmortem = None
            POSTMORTEM_AVAILABLE = False
        
        # State
        self.running = False
        self.last_snapshot_date = None
        
        # Create output directories
        Path("historian_output").mkdir(exist_ok=True)
        Path("historian_output/snapshots").mkdir(exist_ok=True)
        Path("historian_output/reports").mkdir(exist_ok=True)
        Path("historian_output/postmortems").mkdir(exist_ok=True)
    
    def _load_config(self) -> Dict:
        """Load configuration from file"""
        default_config = {
            "lil_enabled": True,
            "wayback_enabled": True,
            "monitoring_interval_hours": 24,
            "max_datasets_per_run": 1000,
            "volatility_threshold": 0.5,
            "change_detection_enabled": True,
            "postmortem_auto_generate": True,
            "api_key": None
        }
        
        try:
            if Path(self.config_file).exists():
                with open(self.config_file, 'r') as f:
                    config = json.load(f)
                    # Merge with defaults
                    for key, value in default_config.items():
                        if key not in config:
                            config[key] = value
                    return config
            else:
                # Create default config file
                with open(self.config_file, 'w') as f:
                    json.dump(default_config, f, indent=2)
                return default_config
        except Exception as e:
            logger.error(f"Error loading config: {e}")
            return default_config
    
    def save_config(self):
        """Save current configuration"""
        try:
            with open(self.config_file, 'w') as f:
                json.dump(self.config, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving config: {e}")
    
    async def run_full_analysis(self) -> Dict:
        """Run a complete analysis cycle"""
        logger.info("Starting full analysis cycle")
        start_time = time.time()
        
        results = {
            'start_time': datetime.now().isoformat(),
            'lil_snapshots': 0,
            'live_snapshots': 0,
            'wayback_snapshots': 0,
            'diffs_generated': 0,
            'postmortems_generated': 0,
            'errors': []
        }
        
        try:
            # Step 1: Discover and process LIL snapshots
            if self.config['lil_enabled']:
                logger.info("Discovering LIL snapshots...")
                lil_snapshots = self.lil.discover_available_snapshots()
                results['lil_snapshots'] = len(lil_snapshots)
                
                for snapshot_info in lil_snapshots:
                    try:
                        success = self.lil.process_snapshot(snapshot_info)
                        if success:
                            logger.info(f"Processed LIL snapshot: {snapshot_info['snapshot_date']}")
                    except Exception as e:
                        logger.error(f"Error processing LIL snapshot {snapshot_info['snapshot_date']}: {e}")
                        results['errors'].append(f"LIL snapshot {snapshot_info['snapshot_date']}: {str(e)}")
            
            # Step 2: Run enhanced monitor for live data
            logger.info("Running enhanced monitor for live data...")
            try:
                live_datasets = await self.enhanced_monitor.fetch_all_datasets()
                results['live_snapshots'] = len(live_datasets)
                
                # Process live datasets
                await self.enhanced_monitor.analyze_datasets_async(live_datasets[:self.config['max_datasets_per_run']])
                logger.info(f"Processed {len(live_datasets)} live datasets")
                
            except Exception as e:
                logger.error(f"Error processing live datasets: {e}")
                results['errors'].append(f"Live datasets: {str(e)}")
            
            # Step 3: Generate diffs and detect changes
            if self.config['change_detection_enabled']:
                logger.info("Generating diffs and detecting changes...")
                try:
                    diffs_generated = await self._generate_all_diffs()
                    results['diffs_generated'] = diffs_generated
                    logger.info(f"Generated {diffs_generated} diffs")
                except Exception as e:
                    logger.error(f"Error generating diffs: {e}")
                    results['errors'].append(f"Diffs: {str(e)}")
            
            # Step 4: Generate post-mortems for high-volatility datasets
            if self.config['postmortem_auto_generate'] and self.postmortem:
                logger.info("Generating post-mortems for high-volatility datasets...")
                try:
                    high_volatility_datasets = self._get_high_volatility_datasets()
                    if high_volatility_datasets:
                        postmortem_results = self.postmortem.generate_batch_postmortems(high_volatility_datasets)
                        results['postmortems_generated'] = len(postmortem_results['successful'])
                        logger.info(f"Generated {len(postmortem_results['successful'])} post-mortems")
                except Exception as e:
                    logger.error(f"Error generating post-mortems: {e}")
                    results['errors'].append(f"Post-mortems: {str(e)}")
            elif self.config['postmortem_auto_generate'] and not self.postmortem:
                logger.warning("Post-mortem generation requested but not available (missing dependencies)")
            
            # Step 5: Process vanished datasets
            if self.config.get('vanished_dataset_reconstruction', True):
                logger.info("Processing vanished datasets...")
                try:
                    vanished_results = self._process_vanished_datasets()
                    results['vanished_datasets_processed'] = vanished_results.get('successful_reconstructions', 0)
                    results['vanished_datasets_failed'] = vanished_results.get('failed_reconstructions', 0)
                except Exception as e:
                    logger.error(f"Error processing vanished datasets: {e}")
                    results['errors'].append(f"Vanished datasets: {str(e)}")
            
            # Step 6: Generate summary reports
            logger.info("Generating summary reports...")
            try:
                self._generate_summary_reports()
            except Exception as e:
                logger.error(f"Error generating summary reports: {e}")
                results['errors'].append(f"Summary reports: {str(e)}")
            
        except Exception as e:
            logger.error(f"Error in full analysis cycle: {e}")
            results['errors'].append(f"Full analysis: {str(e)}")
        
        results['end_time'] = datetime.now().isoformat()
        results['duration_seconds'] = time.time() - start_time
        
        logger.info(f"Analysis cycle completed in {results['duration_seconds']:.2f} seconds")
        return results
    
    async def _generate_all_diffs(self) -> int:
        """Generate diffs for all datasets"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get all datasets
        cursor.execute('SELECT DISTINCT dataset_id FROM historian_snapshots')
        dataset_ids = [row[0] for row in cursor.fetchall()]
        
        diffs_generated = 0
        
        for dataset_id in dataset_ids:
            try:
                # Get snapshots for this dataset
                snapshots = self.historian.get_snapshots(dataset_id)
                
                if len(snapshots) < 2:
                    continue
                
                # Generate diffs between consecutive snapshots
                for i in range(1, len(snapshots)):
                    from_snapshot = snapshots[i-1]
                    to_snapshot = snapshots[i]
                    
                    # Check if diff already exists
                    cursor.execute('''
                        SELECT id FROM historian_diffs
                        WHERE dataset_id = ? AND from_date = ? AND to_date = ?
                    ''', (dataset_id, from_snapshot.snapshot_date, to_snapshot.snapshot_date))
                    
                    if cursor.fetchone():
                        continue
                    
                    # Generate diff
                    diff = self.historian.compute_diff(from_snapshot, to_snapshot)
                    
                    # Store diff
                    if self.historian.store_diff(diff):
                        diffs_generated += 1
                        
                        # Store volatility metrics
                        self._store_volatility_metrics(dataset_id, diff)
                
            except Exception as e:
                logger.error(f"Error generating diffs for {dataset_id}: {e}")
                continue
        
        conn.close()
        return diffs_generated
    
    def _store_volatility_metrics(self, dataset_id: str, diff):
        """Store volatility metrics for a dataset"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Store overall volatility
            cursor.execute('''
                INSERT INTO volatility_metrics
                (dataset_id, metric_name, metric_value, snapshot_date)
                VALUES (?, ?, ?, ?)
            ''', (dataset_id, 'overall_volatility', diff.volatility_score, diff.to_date))
            
            # Store change counts
            cursor.execute('''
                INSERT INTO volatility_metrics
                (dataset_id, metric_name, metric_value, snapshot_date)
                VALUES (?, ?, ?, ?)
            ''', (dataset_id, 'metadata_changes', len(diff.metadata_changes), diff.to_date))
            
            cursor.execute('''
                INSERT INTO volatility_metrics
                (dataset_id, metric_name, metric_value, snapshot_date)
                VALUES (?, ?, ?, ?)
            ''', (dataset_id, 'schema_changes', len(diff.schema_changes), diff.to_date))
            
            # Store content drift
            content_drift = diff.content_changes.get('content_drift', 0.0)
            cursor.execute('''
                INSERT INTO volatility_metrics
                (dataset_id, metric_name, metric_value, snapshot_date)
                VALUES (?, ?, ?, ?)
            ''', (dataset_id, 'content_drift', content_drift, diff.to_date))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"Error storing volatility metrics for {dataset_id}: {e}")
    
    def _get_high_volatility_datasets(self) -> List[str]:
        """Get datasets with high volatility scores"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT dataset_id, AVG(volatility_score) as avg_volatility
                FROM historian_diffs
                GROUP BY dataset_id
                HAVING avg_volatility > ?
                ORDER BY avg_volatility DESC
                LIMIT 20
            ''', (self.config['volatility_threshold'],))
            
            high_volatility = [row[0] for row in cursor.fetchall()]
            conn.close()
            
            return high_volatility
            
        except Exception as e:
            logger.error(f"Error getting high volatility datasets: {e}")
            return []
    
    def _generate_summary_reports(self):
        """Generate summary reports"""
        try:
            # Generate volatility ranking
            ranking = self.timeline_ui.generate_volatility_ranking(100)
            
            # Save ranking report
            ranking_file = Path("historian_output/reports/volatility_ranking.json")
            with open(ranking_file, 'w') as f:
                json.dump(ranking, f, indent=2)
            
            # Generate system status
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('SELECT COUNT(DISTINCT dataset_id) FROM historian_snapshots')
            total_datasets = cursor.fetchone()[0] or 0
            
            cursor.execute('SELECT COUNT(*) FROM historian_snapshots')
            total_snapshots = cursor.fetchone()[0] or 0
            
            cursor.execute('SELECT COUNT(*) FROM historian_diffs')
            total_diffs = cursor.fetchone()[0] or 0
            
            conn.close()
            
            status_report = {
                'timestamp': datetime.now().isoformat(),
                'total_datasets': total_datasets,
                'total_snapshots': total_snapshots,
                'total_diffs': total_diffs,
                'top_volatile_datasets': ranking[:10]
            }
            
            status_file = Path("historian_output/reports/system_status.json")
            with open(status_file, 'w') as f:
                json.dump(status_report, f, indent=2)
            
            logger.info("Summary reports generated")
            
        except Exception as e:
            logger.error(f"Error generating summary reports: {e}")
    
    def _process_vanished_datasets(self) -> Dict:
        """Process vanished datasets and reconstruct their history"""
        try:
            # Detect vanished datasets
            vanished_datasets = self.wayback_fetcher.detect_vanished_datasets()
            
            if not vanished_datasets:
                logger.info("No vanished datasets detected")
                return {'successful_reconstructions': 0, 'failed_reconstructions': 0}
            
            logger.info(f"Found {len(vanished_datasets)} vanished datasets")
            
            # Reconstruct each vanished dataset
            results = {
                'successful_reconstructions': 0,
                'failed_reconstructions': 0,
                'errors': []
            }
            
            for vanished_info in vanished_datasets:
                try:
                    success = self.wayback_fetcher.reconstruct_vanished_dataset(
                        vanished_info['dataset_id'], 
                        vanished_info
                    )
                    
                    if success:
                        results['successful_reconstructions'] += 1
                        logger.info(f"Successfully reconstructed {vanished_info['dataset_id']}")
                    else:
                        results['failed_reconstructions'] += 1
                        logger.warning(f"Failed to reconstruct {vanished_info['dataset_id']}")
                        
                except Exception as e:
                    error_msg = f"Error reconstructing {vanished_info['dataset_id']}: {e}"
                    logger.error(error_msg)
                    results['errors'].append(error_msg)
                    results['failed_reconstructions'] += 1
            
            # Generate diffs for newly reconstructed datasets
            self._generate_vanished_dataset_diffs()
            
            return results
            
        except Exception as e:
            logger.error(f"Error processing vanished datasets: {e}")
            return {'successful_reconstructions': 0, 'failed_reconstructions': 0, 'errors': [str(e)]}
    
    def _generate_vanished_dataset_diffs(self):
        """Generate diffs for vanished datasets"""
        try:
            vanished_datasets = self.historian.get_vanished_datasets()
            
            for vanished_info in vanished_datasets:
                dataset_id = vanished_info['dataset_id']
                
                # Get snapshots for this vanished dataset
                snapshots = self.historian.get_snapshots(dataset_id)
                
                if len(snapshots) < 2:
                    continue
                
                # Generate diffs between consecutive snapshots
                for i in range(1, len(snapshots)):
                    from_snapshot = snapshots[i-1]
                    to_snapshot = snapshots[i]
                    
                    # Check if diff already exists
                    conn = sqlite3.connect(self.db_path)
                    cursor = conn.cursor()
                    
                    cursor.execute('''
                        SELECT id FROM historian_diffs
                        WHERE dataset_id = ? AND from_date = ? AND to_date = ?
                    ''', (dataset_id, from_snapshot.snapshot_date, to_snapshot.snapshot_date))
                    
                    if cursor.fetchone():
                        conn.close()
                        continue
                    
                    conn.close()
                    
                    # Generate diff
                    diff = self.historian.compute_diff(from_snapshot, to_snapshot)
                    
                    # Store diff
                    if self.historian.store_diff(diff):
                        # Store volatility metrics
                        self._store_volatility_metrics(dataset_id, diff)
                        
        except Exception as e:
            logger.error(f"Error generating vanished dataset diffs: {e}")
    
    def start_monitoring(self):
        """Start continuous monitoring"""
        logger.info("Starting continuous monitoring")
        self.running = True
        
        # Schedule regular analysis
        schedule.every(self.config['monitoring_interval_hours']).hours.do(
            lambda: asyncio.run(self.run_full_analysis())
        )
        
        # Run initial analysis
        asyncio.run(self.run_full_analysis())
        
        # Start scheduler in background thread
        def run_scheduler():
            while self.running:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
        
        scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
        scheduler_thread.start()
        
        logger.info(f"Monitoring started - analysis every {self.config['monitoring_interval_hours']} hours")
    
    def stop_monitoring(self):
        """Stop continuous monitoring"""
        logger.info("Stopping monitoring")
        self.running = False
    
    def run_analysis_once(self):
        """Run analysis once and exit"""
        logger.info("Running one-time analysis")
        return asyncio.run(self.run_full_analysis())
    
    def get_system_status(self) -> Dict:
        """Get current system status"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get basic counts
            cursor.execute('SELECT COUNT(DISTINCT dataset_id) FROM historian_snapshots')
            total_datasets = cursor.fetchone()[0] or 0
            
            cursor.execute('SELECT COUNT(*) FROM historian_snapshots')
            total_snapshots = cursor.fetchone()[0] or 0
            
            cursor.execute('SELECT COUNT(*) FROM historian_diffs')
            total_diffs = cursor.fetchone()[0] or 0
            
            cursor.execute('SELECT COUNT(*) FROM lil_snapshots')
            lil_snapshots = cursor.fetchone()[0] or 0
            
            cursor.execute('SELECT COUNT(*) FROM wayback_snapshots')
            wayback_snapshots = cursor.fetchone()[0] or 0
            
            # Get recent activity
            cursor.execute('''
                SELECT MAX(created_at) FROM historian_snapshots
            ''')
            last_snapshot = cursor.fetchone()[0]
            
            conn.close()
            
            return {
                'total_datasets': total_datasets,
                'total_snapshots': total_snapshots,
                'total_diffs': total_diffs,
                'lil_snapshots': lil_snapshots,
                'wayback_snapshots': wayback_snapshots,
                'last_snapshot': last_snapshot,
                'config': self.config,
                'running': self.running
            }
            
        except Exception as e:
            logger.error(f"Error getting system status: {e}")
            return {'error': str(e)}

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Dataset State Historian')
    parser.add_argument('--mode', choices=['once', 'monitor', 'status'], default='once',
                       help='Run mode: once (single analysis), monitor (continuous), status (show status)')
    parser.add_argument('--config', default='historian_config.json',
                       help='Configuration file path')
    parser.add_argument('--db', default='datasets.db',
                       help='Database file path')
    
    args = parser.parse_args()
    
    # Initialize historian
    historian = DatasetHistorianMain(db_path=args.db, config_file=args.config)
    
    if args.mode == 'status':
        status = historian.get_system_status()
        print(json.dumps(status, indent=2))
    
    elif args.mode == 'once':
        results = historian.run_analysis_once()
        print(json.dumps(results, indent=2))
    
    elif args.mode == 'monitor':
        try:
            historian.start_monitoring()
            # Keep running
            while True:
                time.sleep(60)
        except KeyboardInterrupt:
            historian.stop_monitoring()
            print("Monitoring stopped")

if __name__ == '__main__':
    main()
