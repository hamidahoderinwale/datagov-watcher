"""
Worker Pool for Dataset Monitoring
Implements worker-based processing to handle throughput efficiently while respecting rate limits
"""

import asyncio
import logging
import time
from typing import List, Dict, Optional, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
import aiohttp
from .rate_limiter import make_request_with_backoff

logger = logging.getLogger(__name__)

@dataclass
class MonitoringTask:
    """Represents a monitoring task for a dataset"""
    dataset_id: str
    url: str
    priority: str
    last_checked: Optional[datetime] = None
    retry_count: int = 0
    max_retries: int = 3

class WorkerPool:
    """Worker pool for handling dataset monitoring tasks"""
    
    def __init__(self, max_workers: int = 10, requests_per_hour: int = 30):
        self.max_workers = max_workers
        self.requests_per_hour = requests_per_hour
        self.workers = []
        self.task_queue = asyncio.Queue()
        self.running = False
        self.completed_tasks = 0
        self.failed_tasks = 0
        self.start_time = None
        
    async def start(self):
        """Start the worker pool"""
        if self.running:
            return
            
        self.running = True
        self.start_time = datetime.now()
        
        # Create worker tasks
        self.workers = [
            asyncio.create_task(self._worker(f"worker-{i}"))
            for i in range(self.max_workers)
        ]
        
        logger.info(f"Started worker pool with {self.max_workers} workers")
    
    async def stop(self):
        """Stop the worker pool"""
        if not self.running:
            return
            
        self.running = False
        
        # Cancel all workers
        for worker in self.workers:
            worker.cancel()
        
        # Wait for workers to finish
        await asyncio.gather(*self.workers, return_exceptions=True)
        
        logger.info(f"Stopped worker pool. Completed: {self.completed_tasks}, Failed: {self.failed_tasks}")
    
    async def add_task(self, task: MonitoringTask):
        """Add a monitoring task to the queue"""
        await self.task_queue.put(task)
    
    async def add_tasks(self, tasks: List[MonitoringTask]):
        """Add multiple monitoring tasks to the queue"""
        for task in tasks:
            await self.task_queue.put(task)
    
    async def _worker(self, worker_name: str):
        """Worker coroutine that processes tasks from the queue"""
        logger.info(f"{worker_name} started")
        
        async with aiohttp.ClientSession() as session:
            while self.running:
                try:
                    # Get task from queue with timeout
                    task = await asyncio.wait_for(
                        self.task_queue.get(), 
                        timeout=1.0
                    )
                    
                    # Process the task
                    await self._process_task(session, task, worker_name)
                    
                    # Mark task as done
                    self.task_queue.task_done()
                    
                except asyncio.TimeoutError:
                    # No tasks available, continue
                    continue
                except Exception as e:
                    logger.error(f"{worker_name} error: {e}")
                    await asyncio.sleep(1)
        
        logger.info(f"{worker_name} stopped")
    
    async def _process_task(self, session: aiohttp.ClientSession, task: MonitoringTask, worker_name: str):
        """Process a single monitoring task"""
        try:
            logger.debug(f"{worker_name} processing {task.dataset_id}")
            
            # Make request with rate limiting
            response, headers = await make_request_with_backoff(
                session, 
                task.url,
                timeout=aiohttp.ClientTimeout(total=30)
            )
            
            # Process response
            status_code = response.status
            content_length = headers.get('content-length', '0')
            content_type = headers.get('content-type', 'unknown')
            
            # Update database with results
            await self._update_monitoring_result(task, status_code, content_length, content_type)
            
            self.completed_tasks += 1
            logger.debug(f"{worker_name} completed {task.dataset_id}: {status_code}")
            
        except Exception as e:
            logger.error(f"{worker_name} failed {task.dataset_id}: {e}")
            self.failed_tasks += 1
            
            # Retry logic
            if task.retry_count < task.max_retries:
                task.retry_count += 1
                await asyncio.sleep(2 ** task.retry_count)  # Exponential backoff
                await self.add_task(task)
    
    async def _update_monitoring_result(self, task: MonitoringTask, status_code: int, 
                                      content_length: str, content_type: str):
        """Update the database with monitoring results"""
        # This would typically update the live_monitoring table
        # For now, we'll just log the result
        logger.info(f"Monitoring result for {task.dataset_id}: {status_code}, {content_length} bytes, {content_type}")
    
    def get_stats(self) -> Dict:
        """Get worker pool statistics"""
        if not self.start_time:
            return {}
        
        runtime = datetime.now() - self.start_time
        tasks_per_hour = (self.completed_tasks / runtime.total_seconds()) * 3600 if runtime.total_seconds() > 0 else 0
        
        return {
            'running': self.running,
            'max_workers': self.max_workers,
            'active_workers': len([w for w in self.workers if not w.done()]),
            'queue_size': self.task_queue.qsize(),
            'completed_tasks': self.completed_tasks,
            'failed_tasks': self.failed_tasks,
            'runtime_seconds': runtime.total_seconds(),
            'tasks_per_hour': tasks_per_hour,
            'requests_per_hour_limit': self.requests_per_hour
        }

class MonitoringOrchestrator:
    """Orchestrates monitoring tasks across multiple worker pools"""
    
    def __init__(self):
        self.worker_pools = {}
        self.running = False
        
    async def start(self):
        """Start all worker pools"""
        if self.running:
            return
            
        self.running = True
        
        # Create worker pools for different priorities
        self.worker_pools = {
            'critical': WorkerPool(max_workers=5, requests_per_hour=15),  # 15/hour for critical
            'high': WorkerPool(max_workers=3, requests_per_hour=10),      # 10/hour for high
            'medium': WorkerPool(max_workers=2, requests_per_hour=4),     # 4/hour for medium
            'low': WorkerPool(max_workers=1, requests_per_hour=1)         # 1/hour for low
        }
        
        # Start all worker pools
        for pool in self.worker_pools.values():
            await pool.start()
        
        logger.info("Started monitoring orchestrator with worker pools")
    
    async def stop(self):
        """Stop all worker pools"""
        if not self.running:
            return
            
        self.running = False
        
        # Stop all worker pools
        for pool in self.worker_pools.values():
            await pool.stop()
        
        logger.info("Stopped monitoring orchestrator")
    
    async def add_monitoring_tasks(self, tasks: List[MonitoringTask]):
        """Add monitoring tasks to appropriate worker pools"""
        for task in tasks:
            if task.priority in self.worker_pools:
                await self.worker_pools[task.priority].add_task(task)
            else:
                logger.warning(f"Unknown priority {task.priority} for task {task.dataset_id}")
    
    def get_overall_stats(self) -> Dict:
        """Get overall statistics from all worker pools"""
        stats = {
            'running': self.running,
            'worker_pools': {}
        }
        
        for priority, pool in self.worker_pools.items():
            stats['worker_pools'][priority] = pool.get_stats()
        
        return stats

# Global orchestrator instance
monitoring_orchestrator = MonitoringOrchestrator()




