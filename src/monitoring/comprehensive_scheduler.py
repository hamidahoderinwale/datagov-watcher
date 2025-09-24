"""
Comprehensive Monitoring Scheduler
Ensures all datasets are monitored regularly with different frequencies based on importance
"""

import asyncio
import sqlite3
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import time
import json
from pathlib import Path

logger = logging.getLogger(__name__)

class ComprehensiveScheduler:
    """Comprehensive monitoring scheduler with priority-based monitoring"""
    
    def __init__(self, db_path: str = "datasets.db"):
        self.db_path = db_path
        self.running = False
        self.monitoring_tasks = {}
        self.init_database()
        
        # Monitoring priorities and frequencies
        self.monitoring_config = {
            'critical': {
                'frequency_hours': 1,  # Every hour
                'max_workers': 50,
                'timeout': 30,
                'description': 'Critical datasets (high volatility, government priority)'
            },
            'high': {
                'frequency_hours': 6,  # Every 6 hours
                'max_workers': 30,
                'timeout': 20,
                'description': 'High priority datasets (frequently changing)'
            },
            'medium': {
                'frequency_hours': 24,  # Daily
                'max_workers': 20,
                'timeout': 15,
                'description': 'Medium priority datasets (moderate activity)'
            },
            'low': {
                'frequency_hours': 168,  # Weekly
                'max_workers': 10,
                'timeout': 10,
                'description': 'Low priority datasets (stable, infrequent changes)'
            }
        }
        
        # Dataset classification rules
        self.classification_rules = {
            'critical': [
                'agency LIKE "%Census%"',
                'agency LIKE "%Bureau%"',
                'title LIKE "%population%"',
                'title LIKE "%economic%"',
                'title LIKE "%financial%"',
                'volatility_score > 0.8',
                'change_frequency > 0.5'
            ],
            'high': [
                'agency LIKE "%Health%"',
                'agency LIKE "%Environment%"',
                'agency LIKE "%Transportation%"',
                'volatility_score > 0.5',
                'change_frequency > 0.3',
                'last_modified > datetime("now", "-7 days")'
            ],
            'medium': [
                'agency LIKE "%Education%"',
                'agency LIKE "%Agriculture%"',
                'volatility_score > 0.2',
                'change_frequency > 0.1',
                'last_modified > datetime("now", "-30 days")'
            ]
        }
    
    def init_database(self):
        """Initialize database tables for comprehensive monitoring"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Monitoring schedule table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS monitoring_schedule (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id TEXT NOT NULL,
                priority TEXT NOT NULL,
                next_check TIMESTAMP NOT NULL,
                frequency_hours INTEGER NOT NULL,
                last_check TIMESTAMP,
                check_count INTEGER DEFAULT 0,
                success_count INTEGER DEFAULT 0,
                failure_count INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(dataset_id)
            )
        ''')
        
        # Monitoring results table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS monitoring_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id TEXT NOT NULL,
                check_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT NOT NULL,
                response_time_ms INTEGER,
                status_code INTEGER,
                content_hash TEXT,
                change_detected BOOLEAN DEFAULT FALSE,
                error_message TEXT,
                metadata TEXT  -- JSON
            )
        ''')
        
        # Monitoring statistics table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS monitoring_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                priority TEXT NOT NULL,
                total_checks INTEGER DEFAULT 0,
                successful_checks INTEGER DEFAULT 0,
                failed_checks INTEGER DEFAULT 0,
                avg_response_time_ms REAL DEFAULT 0,
                changes_detected INTEGER DEFAULT 0,
                UNIQUE(date, priority)
            )
        ''')
        
        conn.commit()
        conn.close()
    
    async def initialize_monitoring_schedule(self):
        """Initialize monitoring schedule for all datasets"""
        logger.info("Initializing comprehensive monitoring schedule")
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get all unique datasets
        cursor.execute('''
            SELECT DISTINCT ds.dataset_id, ds.title, ds.agency, 
                   0 as volatility_score,
                   COALESCE(lm.change_frequency, 0) as change_frequency,
                   ds.last_modified
            FROM dataset_states ds
            LEFT JOIN (
                SELECT dataset_id, 
                       COUNT(*) * 1.0 / (julianday('now') - julianday(MIN(last_checked))) as change_frequency
                FROM live_monitoring 
                WHERE last_checked IS NOT NULL
                GROUP BY dataset_id
            ) lm ON ds.dataset_id = lm.dataset_id
            WHERE ds.dataset_id IN (
                SELECT dataset_id 
                FROM dataset_states 
                GROUP BY dataset_id 
                HAVING MAX(created_at)
            )
        ''')
        
        datasets = cursor.fetchall()
        logger.info(f"Found {len(datasets)} datasets to schedule")
        
        # Classify and schedule each dataset
        scheduled_count = 0
        for dataset in datasets:
            dataset_id, title, agency, volatility_score, change_frequency, last_modified = dataset
            
            # Determine priority
            priority = self._classify_dataset_priority(
                title, agency, volatility_score, change_frequency, last_modified
            )
            
            # Calculate next check time
            frequency_hours = self.monitoring_config[priority]['frequency_hours']
            next_check = datetime.now() + timedelta(hours=frequency_hours)
            
            # Insert or update schedule
            cursor.execute('''
                INSERT OR REPLACE INTO monitoring_schedule 
                (dataset_id, priority, next_check, frequency_hours, created_at)
                VALUES (?, ?, ?, ?, ?)
            ''', (dataset_id, priority, next_check, frequency_hours, datetime.now()))
            
            scheduled_count += 1
        
        conn.commit()
        conn.close()
        
        logger.info(f"Scheduled {scheduled_count} datasets for monitoring")
        return scheduled_count
    
    def _classify_dataset_priority(self, title: str, agency: str, 
                                 volatility_score: float, change_frequency: float, 
                                 last_modified: str) -> str:
        """Classify dataset priority based on rules"""
        # Check critical rules
        if self._check_critical_rules(title, agency, volatility_score, change_frequency, last_modified):
            return 'critical'
        
        # Check high rules
        if self._check_high_rules(title, agency, volatility_score, change_frequency, last_modified):
            return 'high'
        
        # Check medium rules
        if self._check_medium_rules(title, agency, volatility_score, change_frequency, last_modified):
            return 'medium'
        
        return 'low'
    
    def _check_critical_rules(self, title: str, agency: str, volatility_score: float, 
                            change_frequency: float, last_modified: str) -> bool:
        """Check critical priority rules"""
        if not title or not agency:
            return False
            
        # Agency-based rules
        if any(keyword in agency.lower() for keyword in ['census', 'bureau']):
            return True
            
        # Title-based rules
        if any(keyword in title.lower() for keyword in ['population', 'economic', 'financial']):
            return True
            
        # Volatility rules
        if volatility_score > 0.8:
            return True
            
        # Change frequency rules
        if change_frequency > 0.5:
            return True
            
        return False
    
    def _check_high_rules(self, title: str, agency: str, volatility_score: float, 
                         change_frequency: float, last_modified: str) -> bool:
        """Check high priority rules"""
        if not title or not agency:
            return False
            
        # Agency-based rules
        if any(keyword in agency.lower() for keyword in ['health', 'environment', 'transportation']):
            return True
            
        # Volatility rules
        if volatility_score > 0.5:
            return True
            
        # Change frequency rules
        if change_frequency > 0.3:
            return True
            
        return False
    
    def _check_medium_rules(self, title: str, agency: str, volatility_score: float, 
                           change_frequency: float, last_modified: str) -> bool:
        """Check medium priority rules"""
        if not title or not agency:
            return False
            
        # Agency-based rules
        if any(keyword in agency.lower() for keyword in ['education', 'agriculture']):
            return True
            
        # Volatility rules
        if volatility_score > 0.2:
            return True
            
        # Change frequency rules
        if change_frequency > 0.1:
            return True
            
        return False
    
    
    async def start_monitoring(self):
        """Start comprehensive monitoring"""
        logger.info("Starting comprehensive monitoring system")
        self.running = True
        
        # Initialize schedule if needed
        await self.initialize_monitoring_schedule()
        
        # Start monitoring tasks for each priority
        for priority, config in self.monitoring_config.items():
            task = asyncio.create_task(
                self._monitor_priority(priority, config)
            )
            self.monitoring_tasks[priority] = task
        
        # Start statistics collection
        stats_task = asyncio.create_task(self._collect_statistics())
        self.monitoring_tasks['stats'] = stats_task
        
        logger.info("Comprehensive monitoring started")
    
    async def stop_monitoring(self):
        """Stop comprehensive monitoring"""
        logger.info("Stopping comprehensive monitoring system")
        self.running = False
        
        # Cancel all tasks
        for task in self.monitoring_tasks.values():
            task.cancel()
        
        self.monitoring_tasks.clear()
        logger.info("Comprehensive monitoring stopped")
    
    async def _monitor_priority(self, priority: str, config: Dict):
        """Monitor datasets of a specific priority"""
        logger.info(f"Starting {priority} priority monitoring")
        
        while self.running:
            try:
                # Get datasets due for monitoring
                due_datasets = await self._get_due_datasets(priority)
                
                if due_datasets:
                    logger.info(f"Monitoring {len(due_datasets)} {priority} priority datasets")
                    
                    # Monitor datasets in parallel
                    await self._monitor_datasets_batch(due_datasets, priority, config)
                
                # Wait for next check
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                logger.error(f"Error in {priority} monitoring: {e}")
                await asyncio.sleep(300)  # Wait 5 minutes on error
    
    async def _get_due_datasets(self, priority: str) -> List[Dict]:
        """Get datasets due for monitoring"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT ms.dataset_id, ds.title, ds.agency, ds.url
            FROM monitoring_schedule ms
            JOIN dataset_states ds ON ms.dataset_id = ds.dataset_id
            WHERE ms.priority = ? AND ms.next_check <= datetime('now')
            ORDER BY ms.next_check ASC
            LIMIT 100
        ''', (priority,))
        
        datasets = [
            {
                'dataset_id': row[0],
                'title': row[1],
                'agency': row[2],
                'url': row[3]
            }
            for row in cursor.fetchall()
        ]
        
        conn.close()
        return datasets
    
    async def _monitor_datasets_batch(self, datasets: List[Dict], priority: str, config: Dict):
        """Monitor a batch of datasets"""
        import aiohttp
        
        async with aiohttp.ClientSession() as session:
            # Create semaphore to limit concurrent requests
            semaphore = asyncio.Semaphore(config['max_workers'])
            
            # Create monitoring tasks
            tasks = [
                self._monitor_single_dataset(semaphore, session, dataset, priority, config)
                for dataset in datasets
            ]
            
            # Wait for all tasks to complete
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _monitor_single_dataset(self, semaphore, session, dataset: Dict, 
                                    priority: str, config: Dict):
        """Monitor a single dataset"""
        async with semaphore:
            dataset_id = dataset['dataset_id']
            url = dataset['url']
            
            start_time = time.time()
            status = 'unknown'
            status_code = None
            response_time_ms = 0
            content_hash = None
            change_detected = False
            error_message = None
            
            try:
                # Make request with timeout
                async with session.get(url, timeout=config['timeout']) as response:
                    status_code = response.status
                    status = 'available' if status_code == 200 else 'unavailable'
                    
                    # Get content hash if successful
                    if status_code == 200:
                        content = await response.read()
                        content_hash = hashlib.md5(content).hexdigest()
                        
                        # Check for changes
                        change_detected = await self._check_for_changes(dataset_id, content_hash)
                    
            except asyncio.TimeoutError:
                status = 'timeout'
                error_message = 'Request timeout'
            except Exception as e:
                status = 'error'
                error_message = str(e)
            
            response_time_ms = int((time.time() - start_time) * 1000)
            
            # Record result
            await self._record_monitoring_result(
                dataset_id, status, response_time_ms, status_code, 
                content_hash, change_detected, error_message
            )
            
            # Update schedule
            await self._update_monitoring_schedule(dataset_id, priority, config)
    
    async def _check_for_changes(self, dataset_id: str, content_hash: str) -> bool:
        """Check if dataset content has changed"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get last known content hash
        cursor.execute('''
            SELECT content_hash FROM live_monitoring 
            WHERE dataset_id = ? 
            ORDER BY last_checked DESC 
            LIMIT 1
        ''', (dataset_id,))
        
        result = cursor.fetchone()
        last_hash = result[0] if result else None
        
        conn.close()
        
        return last_hash != content_hash if last_hash else True
    
    async def _record_monitoring_result(self, dataset_id: str, status: str, 
                                      response_time_ms: int, status_code: int,
                                      content_hash: str, change_detected: bool, 
                                      error_message: str):
        """Record monitoring result"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Insert monitoring result
        cursor.execute('''
            INSERT INTO monitoring_results 
            (dataset_id, status, response_time_ms, status_code, content_hash, 
             change_detected, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (dataset_id, status, response_time_ms, status_code, content_hash, 
              change_detected, error_message))
        
        # Update live monitoring
        cursor.execute('''
            INSERT OR REPLACE INTO live_monitoring 
            (dataset_id, last_checked, status, response_time_ms, content_hash, change_detected)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (dataset_id, datetime.now(), status, response_time_ms, content_hash, change_detected))
        
        conn.commit()
        conn.close()
    
    async def _update_monitoring_schedule(self, dataset_id: str, priority: str, config: Dict):
        """Update monitoring schedule for next check"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Calculate next check time
        next_check = datetime.now() + timedelta(hours=config['frequency_hours'])
        
        # Update schedule
        cursor.execute('''
            UPDATE monitoring_schedule 
            SET next_check = ?, last_check = datetime('now'), check_count = check_count + 1,
                success_count = CASE WHEN ? = 'available' THEN success_count + 1 ELSE success_count END,
                failure_count = CASE WHEN ? != 'available' THEN failure_count + 1 ELSE failure_count END
            WHERE dataset_id = ?
        ''', (next_check, status, status, dataset_id))
        
        conn.commit()
        conn.close()
    
    async def _collect_statistics(self):
        """Collect monitoring statistics"""
        while self.running:
            try:
                await self._update_daily_statistics()
                await asyncio.sleep(3600)  # Update every hour
            except Exception as e:
                logger.error(f"Error collecting statistics: {e}")
                await asyncio.sleep(3600)
    
    async def _update_daily_statistics(self):
        """Update daily monitoring statistics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        today = datetime.now().strftime('%Y-%m-%d')
        
        for priority in self.monitoring_config.keys():
            # Get statistics for today
            cursor.execute('''
                SELECT 
                    COUNT(*) as total_checks,
                    SUM(CASE WHEN status = 'available' THEN 1 ELSE 0 END) as successful_checks,
                    SUM(CASE WHEN status != 'available' THEN 1 ELSE 0 END) as failed_checks,
                    AVG(response_time_ms) as avg_response_time,
                    SUM(CASE WHEN change_detected = 1 THEN 1 ELSE 0 END) as changes_detected
                FROM monitoring_results mr
                JOIN monitoring_schedule ms ON mr.dataset_id = ms.dataset_id
                WHERE ms.priority = ? AND DATE(mr.check_time) = ?
            ''', (priority, today))
            
            result = cursor.fetchone()
            if result:
                total_checks, successful_checks, failed_checks, avg_response_time, changes_detected = result
                
                # Insert or update statistics
                cursor.execute('''
                    INSERT OR REPLACE INTO monitoring_stats 
                    (date, priority, total_checks, successful_checks, failed_checks, 
                     avg_response_time_ms, changes_detected)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (today, priority, total_checks or 0, successful_checks or 0, 
                      failed_checks or 0, avg_response_time or 0, changes_detected or 0))
        
        conn.commit()
        conn.close()
    
    def get_monitoring_status(self) -> Dict:
        """Get current monitoring status"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get schedule summary
        cursor.execute('''
            SELECT priority, COUNT(*) as total, 
                   SUM(CASE WHEN next_check <= datetime('now') THEN 1 ELSE 0 END) as due,
                   AVG(frequency_hours) as avg_frequency
            FROM monitoring_schedule 
            GROUP BY priority
        ''')
        
        schedule_summary = {
            row[0]: {
                'total': row[1],
                'due': row[2],
                'avg_frequency_hours': row[3]
            }
            for row in cursor.fetchall()
        }
        
        # Get recent activity
        cursor.execute('''
            SELECT priority, COUNT(*) as checks_last_hour
            FROM monitoring_results mr
            JOIN monitoring_schedule ms ON mr.dataset_id = ms.dataset_id
            WHERE mr.check_time > datetime('now', '-1 hour')
            GROUP BY priority
        ''')
        
        recent_activity = {
            row[0]: row[1] for row in cursor.fetchall()
        }
        
        # Get success rates
        cursor.execute('''
            SELECT priority, 
                   AVG(CASE WHEN status = 'available' THEN 1.0 ELSE 0.0 END) as success_rate,
                   AVG(response_time_ms) as avg_response_time
            FROM monitoring_results mr
            JOIN monitoring_schedule ms ON mr.dataset_id = ms.dataset_id
            WHERE mr.check_time > datetime('now', '-24 hours')
            GROUP BY priority
        ''')
        
        success_rates = {
            row[0]: {
                'success_rate': row[1],
                'avg_response_time_ms': row[2]
            }
            for row in cursor.fetchall()
        }
        
        conn.close()
        
        return {
            'running': self.running,
            'schedule_summary': schedule_summary,
            'recent_activity': recent_activity,
            'success_rates': success_rates,
            'monitoring_config': self.monitoring_config
        }

# CLI interface
async def main():
    """Main entry point for comprehensive scheduler"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Comprehensive Monitoring Scheduler')
    parser.add_argument('--mode', choices=['start', 'stop', 'status', 'init'], default='start',
                       help='Operation mode')
    parser.add_argument('--db', default='datasets.db',
                       help='Database file path')
    
    args = parser.parse_args()
    
    scheduler = ComprehensiveScheduler(args.db)
    
    if args.mode == 'init':
        count = await scheduler.initialize_monitoring_schedule()
        print(f"Initialized monitoring for {count} datasets")
    
    elif args.mode == 'start':
        await scheduler.start_monitoring()
        try:
            while True:
                await asyncio.sleep(60)
        except KeyboardInterrupt:
            await scheduler.stop_monitoring()
    
    elif args.mode == 'stop':
        await scheduler.stop_monitoring()
        print("Monitoring stopped")
    
    elif args.mode == 'status':
        status = scheduler.get_monitoring_status()
        print(json.dumps(status, indent=2))

if __name__ == '__main__':
    asyncio.run(main())


