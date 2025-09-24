#!/usr/bin/env python3
"""
Comprehensive Dataset Discovery and Monitoring System
Automatically ensures all datasets are discovered and monitored regularly
"""

import asyncio
import time
import logging
import signal
import sys
from datetime import datetime
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent / 'src'))

from src.core.comprehensive_discovery import ComprehensiveDiscovery
from src.monitoring.comprehensive_scheduler import ComprehensiveScheduler

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ComprehensiveSystem:
    """Main orchestrator for comprehensive dataset discovery and monitoring"""
    
    def __init__(self, db_path: str = "datasets.db"):
        self.db_path = db_path
        self.running = False
        self.discovery = ComprehensiveDiscovery(db_path)
        self.scheduler = ComprehensiveScheduler(db_path)
        
        # Configuration
        self.discovery_interval_hours = 24  # Run discovery daily
        self.monitoring_enabled = True
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, shutting down...")
        self.running = False
    
    async def initialize_system(self):
        """Initialize the comprehensive system"""
        logger.info("Initializing comprehensive dataset discovery and monitoring system")
        
        try:
            # Initialize monitoring schedule
            logger.info("Initializing monitoring schedule...")
            count = await self.scheduler.initialize_monitoring_schedule()
            logger.info(f"Initialized monitoring for {count} datasets")
            
            # Run initial discovery
            logger.info("Running initial dataset discovery...")
            session_id = await self.discovery.start_discovery_session()
            results = await self.discovery.run_comprehensive_discovery(session_id)
            logger.info(f"Initial discovery completed: {results['total_datasets']} total, {results['new_datasets']} new")
            
            return True
            
        except Exception as e:
            logger.error(f"Error initializing system: {e}")
            return False
    
    async def start_system(self):
        """Start the comprehensive system"""
        logger.info("Starting comprehensive dataset discovery and monitoring system")
        
        if not await self.initialize_system():
            logger.error("Failed to initialize system")
            return
        
        self.running = True
        
        # Start monitoring
        if self.monitoring_enabled:
            logger.info("Starting comprehensive monitoring...")
            await self.scheduler.start_monitoring()
        
        # Start discovery scheduler
        discovery_task = asyncio.create_task(self._discovery_scheduler())
        
        # Start status reporting
        status_task = asyncio.create_task(self._status_reporter())
        
        logger.info("Comprehensive system started successfully")
        
        try:
            # Wait for tasks
            await asyncio.gather(discovery_task, status_task)
        except asyncio.CancelledError:
            logger.info("Tasks cancelled")
        finally:
            await self.shutdown()
    
    async def _discovery_scheduler(self):
        """Schedule regular discovery runs"""
        while self.running:
            try:
                # Wait for next discovery interval
                await asyncio.sleep(self.discovery_interval_hours * 3600)
                
                if self.running:
                    logger.info("Starting scheduled discovery...")
                    session_id = await self.discovery.start_discovery_session()
                    results = await self.discovery.run_comprehensive_discovery(session_id)
                    logger.info(f"Scheduled discovery completed: {results['total_datasets']} total, {results['new_datasets']} new")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in discovery scheduler: {e}")
                await asyncio.sleep(300)  # Wait 5 minutes on error
    
    async def _status_reporter(self):
        """Report system status periodically"""
        while self.running:
            try:
                await asyncio.sleep(3600)  # Report every hour
                
                if self.running:
                    # Get discovery stats
                    discovery_stats = self.discovery.get_discovery_stats()
                    
                    # Get monitoring status
                    monitoring_status = self.scheduler.get_monitoring_status()
                    
                    logger.info(f"System Status - Datasets: {discovery_stats['total_datasets']}, "
                              f"Monitoring: {'Running' if monitoring_status['running'] else 'Stopped'}, "
                              f"Recent Discoveries: {discovery_stats['recent_discoveries']}")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in status reporter: {e}")
    
    async def shutdown(self):
        """Shutdown the system gracefully"""
        logger.info("Shutting down comprehensive system...")
        
        try:
            if self.monitoring_enabled:
                await self.scheduler.stop_monitoring()
            
            logger.info("System shutdown completed")
            
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
    
    def get_system_status(self) -> dict:
        """Get current system status"""
        try:
            discovery_stats = self.discovery.get_discovery_stats()
            monitoring_status = self.scheduler.get_monitoring_status()
            
            return {
                'running': self.running,
                'discovery': discovery_stats,
                'monitoring': monitoring_status,
                'uptime': time.time() - getattr(self, 'start_time', time.time()),
                'last_update': datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Error getting system status: {e}")
            return {'error': str(e)}

async def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Comprehensive Dataset Discovery and Monitoring System')
    parser.add_argument('--db', default='datasets.db', help='Database file path')
    parser.add_argument('--discovery-interval', type=int, default=24, 
                       help='Discovery interval in hours (default: 24)')
    parser.add_argument('--no-monitoring', action='store_true', 
                       help='Disable monitoring (discovery only)')
    parser.add_argument('--status', action='store_true', 
                       help='Show system status and exit')
    
    args = parser.parse_args()
    
    system = ComprehensiveSystem(args.db)
    system.discovery_interval_hours = args.discovery_interval
    system.monitoring_enabled = not args.no_monitoring
    
    if args.status:
        status = system.get_system_status()
        print(f"System Status: {status}")
        return
    
    system.start_time = time.time()
    
    try:
        await system.start_system()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"System error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    asyncio.run(main())
