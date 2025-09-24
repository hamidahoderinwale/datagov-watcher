"""
System Monitor and Alerting Component
Monitors system health and sends alerts for critical issues
"""

import asyncio
import sqlite3
import logging
import time
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from pathlib import Path
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

logger = logging.getLogger(__name__)

class SystemMonitor:
    """System health monitoring and alerting"""
    
    def __init__(self, db_path: str = "datasets.db"):
        self.db_path = db_path
        self.running = False
        self.alert_thresholds = {
            'error_rate': 0.1,  # 10% error rate threshold
            'response_time': 30.0,  # 30 second response time threshold
            'availability_rate': 0.95,  # 95% availability threshold
            'disk_usage': 0.9,  # 90% disk usage threshold
            'memory_usage': 0.85,  # 85% memory usage threshold
            'failed_discoveries': 5,  # 5 consecutive failed discoveries
            'monitoring_gaps': 3600,  # 1 hour monitoring gap threshold
        }
        
        # Alert configuration
        self.alert_config = {
            'email_enabled': False,
            'email_smtp_server': 'smtp.gmail.com',
            'email_smtp_port': 587,
            'email_username': '',
            'email_password': '',
            'email_recipients': [],
            'webhook_enabled': False,
            'webhook_url': '',
            'log_alerts': True,
        }
        
        self.init_database()
    
    def init_database(self):
        """Initialize database tables for system monitoring"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # System alerts table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS system_alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                alert_type TEXT NOT NULL,
                severity TEXT NOT NULL,
                message TEXT NOT NULL,
                details TEXT,  -- JSON
                resolved BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                resolved_at TIMESTAMP
            )
        ''')
        
        # System metrics table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS system_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                metric_name TEXT NOT NULL,
                metric_value REAL NOT NULL,
                threshold_value REAL,
                status TEXT NOT NULL,  -- 'ok', 'warning', 'critical'
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # System health history
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS system_health_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                overall_health TEXT NOT NULL,
                metrics_summary TEXT,  -- JSON
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
    
    async def start_monitoring(self, interval_minutes: int = 5):
        """Start system monitoring"""
        logger.info(f"Starting system monitoring (interval: {interval_minutes} minutes)")
        self.running = True
        
        while self.running:
            try:
                await self._check_system_health()
                await asyncio.sleep(interval_minutes * 60)
            except Exception as e:
                logger.error(f"Error in system monitoring: {e}")
                await asyncio.sleep(60)  # Wait 1 minute on error
    
    def stop_monitoring(self):
        """Stop system monitoring"""
        logger.info("Stopping system monitoring")
        self.running = False
    
    async def _check_system_health(self):
        """Check overall system health"""
        logger.debug("Checking system health")
        
        metrics = {}
        alerts = []
        
        # Check monitoring system health
        monitoring_health = await self._check_monitoring_health()
        metrics.update(monitoring_health['metrics'])
        alerts.extend(monitoring_health['alerts'])
        
        # Check discovery system health
        discovery_health = await self._check_discovery_health()
        metrics.update(discovery_health['metrics'])
        alerts.extend(discovery_health['alerts'])
        
        # Check database health
        db_health = await self._check_database_health()
        metrics.update(db_health['metrics'])
        alerts.extend(db_health['alerts'])
        
        # Check system resources
        resource_health = await self._check_resource_health()
        metrics.update(resource_health['metrics'])
        alerts.extend(resource_health['alerts'])
        
        # Determine overall health
        overall_health = self._determine_overall_health(metrics, alerts)
        
        # Store health history
        await self._store_health_history(overall_health, metrics)
        
        # Process alerts
        for alert in alerts:
            await self._process_alert(alert)
        
        logger.info(f"System health check completed: {overall_health}")
        return {
            'overall_health': overall_health,
            'metrics': metrics,
            'alerts': alerts
        }
    
    async def _check_monitoring_health(self) -> Dict:
        """Check monitoring system health"""
        metrics = {}
        alerts = []
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Check monitoring activity in last hour
            cursor.execute('''
                SELECT COUNT(*) as total_checks,
                       SUM(CASE WHEN status = 'available' THEN 1 ELSE 0 END) as successful_checks,
                       AVG(response_time_ms) as avg_response_time
                FROM monitoring_results 
                WHERE check_time > datetime('now', '-1 hour')
            ''')
            
            result = cursor.fetchone()
            if result:
                total_checks, successful_checks, avg_response_time = result
                
                if total_checks > 0:
                    success_rate = successful_checks / total_checks
                    error_rate = 1 - success_rate
                    
                    metrics['monitoring_success_rate'] = success_rate
                    metrics['monitoring_error_rate'] = error_rate
                    metrics['monitoring_avg_response_time'] = avg_response_time or 0
                    metrics['monitoring_total_checks'] = total_checks
                    
                    # Check thresholds
                    if error_rate > self.alert_thresholds['error_rate']:
                        alerts.append({
                            'type': 'monitoring_error_rate',
                            'severity': 'critical',
                            'message': f'High monitoring error rate: {error_rate:.1%}',
                            'details': {'error_rate': error_rate, 'threshold': self.alert_thresholds['error_rate']}
                        })
                    
                    if avg_response_time and avg_response_time > self.alert_thresholds['response_time'] * 1000:
                        alerts.append({
                            'type': 'monitoring_response_time',
                            'severity': 'warning',
                            'message': f'High monitoring response time: {avg_response_time:.0f}ms',
                            'details': {'response_time': avg_response_time, 'threshold': self.alert_thresholds['response_time'] * 1000}
                        })
                else:
                    # No monitoring activity in last hour
                    alerts.append({
                        'type': 'monitoring_inactivity',
                        'severity': 'critical',
                        'message': 'No monitoring activity in the last hour',
                        'details': {'last_check': 'unknown'}
                    })
            
            # Check for monitoring gaps
            cursor.execute('''
                SELECT MAX(check_time) as last_check
                FROM monitoring_results
            ''')
            
            result = cursor.fetchone()
            if result and result[0]:
                last_check = datetime.fromisoformat(result[0])
                gap_seconds = (datetime.now() - last_check).total_seconds()
                
                if gap_seconds > self.alert_thresholds['monitoring_gaps']:
                    alerts.append({
                        'type': 'monitoring_gap',
                        'severity': 'warning',
                        'message': f'Monitoring gap detected: {gap_seconds/3600:.1f} hours',
                        'details': {'gap_seconds': gap_seconds, 'last_check': result[0]}
                    })
            
            conn.close()
            
        except Exception as e:
            logger.error(f"Error checking monitoring health: {e}")
            alerts.append({
                'type': 'monitoring_check_error',
                'severity': 'critical',
                'message': f'Failed to check monitoring health: {str(e)}',
                'details': {'error': str(e)}
            })
        
        return {'metrics': metrics, 'alerts': alerts}
    
    async def _check_discovery_health(self) -> Dict:
        """Check discovery system health"""
        metrics = {}
        alerts = []
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Check recent discovery sessions
            cursor.execute('''
                SELECT COUNT(*) as total_sessions,
                       SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as successful_sessions,
                       MAX(start_time) as last_session
                FROM discovery_sessions 
                WHERE start_time > datetime('now', '-24 hours')
            ''')
            
            result = cursor.fetchone()
            if result:
                total_sessions, successful_sessions, last_session = result
                
                if total_sessions > 0:
                    success_rate = successful_sessions / total_sessions
                    metrics['discovery_success_rate'] = success_rate
                    metrics['discovery_total_sessions'] = total_sessions
                    
                    if success_rate < 0.8:  # Less than 80% success rate
                        alerts.append({
                            'type': 'discovery_success_rate',
                            'severity': 'warning',
                            'message': f'Low discovery success rate: {success_rate:.1%}',
                            'details': {'success_rate': success_rate, 'total_sessions': total_sessions}
                        })
                else:
                    # No discovery sessions in last 24 hours
                    alerts.append({
                        'type': 'discovery_inactivity',
                        'severity': 'warning',
                        'message': 'No discovery sessions in the last 24 hours',
                        'details': {'last_session': last_session}
                    })
            
            conn.close()
            
        except Exception as e:
            logger.error(f"Error checking discovery health: {e}")
            alerts.append({
                'type': 'discovery_check_error',
                'severity': 'critical',
                'message': f'Failed to check discovery health: {str(e)}',
                'details': {'error': str(e)}
            })
        
        return {'metrics': metrics, 'alerts': alerts}
    
    async def _check_database_health(self) -> Dict:
        """Check database health"""
        metrics = {}
        alerts = []
        
        try:
            # Check database file size
            db_path = Path(self.db_path)
            if db_path.exists():
                db_size_mb = db_path.stat().st_size / (1024 * 1024)
                metrics['database_size_mb'] = db_size_mb
                
                if db_size_mb > 1000:  # More than 1GB
                    alerts.append({
                        'type': 'database_size',
                        'severity': 'warning',
                        'message': f'Large database size: {db_size_mb:.1f}MB',
                        'details': {'size_mb': db_size_mb}
                    })
            
            # Check database connectivity and basic operations
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Test basic query
            start_time = time.time()
            cursor.execute('SELECT COUNT(*) FROM dataset_states')
            result = cursor.fetchone()
            query_time = (time.time() - start_time) * 1000
            
            metrics['database_query_time_ms'] = query_time
            metrics['database_total_datasets'] = result[0] if result else 0
            
            if query_time > 5000:  # More than 5 seconds
                alerts.append({
                    'type': 'database_performance',
                    'severity': 'warning',
                    'message': f'Slow database query: {query_time:.0f}ms',
                    'details': {'query_time': query_time}
                })
            
            conn.close()
            
        except Exception as e:
            logger.error(f"Error checking database health: {e}")
            alerts.append({
                'type': 'database_error',
                'severity': 'critical',
                'message': f'Database health check failed: {str(e)}',
                'details': {'error': str(e)}
            })
        
        return {'metrics': metrics, 'alerts': alerts}
    
    async def _check_resource_health(self) -> Dict:
        """Check system resource health"""
        metrics = {}
        alerts = []
        
        try:
            import psutil
            
            # Check disk usage
            disk_usage = psutil.disk_usage('/')
            disk_percent = disk_usage.percent / 100
            metrics['disk_usage_percent'] = disk_percent
            
            if disk_percent > self.alert_thresholds['disk_usage']:
                alerts.append({
                    'type': 'disk_usage',
                    'severity': 'critical',
                    'message': f'High disk usage: {disk_percent:.1%}',
                    'details': {'usage_percent': disk_percent, 'free_gb': disk_usage.free / (1024**3)}
                })
            
            # Check memory usage
            memory = psutil.virtual_memory()
            memory_percent = memory.percent / 100
            metrics['memory_usage_percent'] = memory_percent
            
            if memory_percent > self.alert_thresholds['memory_usage']:
                alerts.append({
                    'type': 'memory_usage',
                    'severity': 'warning',
                    'message': f'High memory usage: {memory_percent:.1%}',
                    'details': {'usage_percent': memory_percent, 'available_gb': memory.available / (1024**3)}
                })
            
            # Check CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)
            metrics['cpu_usage_percent'] = cpu_percent
            
            if cpu_percent > 90:
                alerts.append({
                    'type': 'cpu_usage',
                    'severity': 'warning',
                    'message': f'High CPU usage: {cpu_percent:.1f}%',
                    'details': {'usage_percent': cpu_percent}
                })
            
        except ImportError:
            logger.warning("psutil not available, skipping resource monitoring")
        except Exception as e:
            logger.error(f"Error checking resource health: {e}")
            alerts.append({
                'type': 'resource_check_error',
                'severity': 'warning',
                'message': f'Failed to check resource health: {str(e)}',
                'details': {'error': str(e)}
            })
        
        return {'metrics': metrics, 'alerts': alerts}
    
    def _determine_overall_health(self, metrics: Dict, alerts: List[Dict]) -> str:
        """Determine overall system health"""
        critical_alerts = [a for a in alerts if a['severity'] == 'critical']
        warning_alerts = [a for a in alerts if a['severity'] == 'warning']
        
        if critical_alerts:
            return 'critical'
        elif warning_alerts:
            return 'warning'
        else:
            return 'healthy'
    
    async def _store_health_history(self, overall_health: str, metrics: Dict):
        """Store system health history"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO system_health_history (overall_health, metrics_summary)
                VALUES (?, ?)
            ''', (overall_health, json.dumps(metrics)))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"Error storing health history: {e}")
    
    async def _process_alert(self, alert: Dict):
        """Process and send alert"""
        try:
            # Store alert in database
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO system_alerts (alert_type, severity, message, details)
                VALUES (?, ?, ?, ?)
            ''', (alert['type'], alert['severity'], alert['message'], json.dumps(alert.get('details', {}))))
            
            conn.commit()
            conn.close()
            
            # Log alert
            if self.alert_config['log_alerts']:
                logger.warning(f"ALERT [{alert['severity'].upper()}] {alert['type']}: {alert['message']}")
            
            # Send email alert
            if self.alert_config['email_enabled'] and alert['severity'] == 'critical':
                await self._send_email_alert(alert)
            
            # Send webhook alert
            if self.alert_config['webhook_enabled']:
                await self._send_webhook_alert(alert)
            
        except Exception as e:
            logger.error(f"Error processing alert: {e}")
    
    async def _send_email_alert(self, alert: Dict):
        """Send email alert"""
        try:
            msg = MIMEMultipart()
            msg['From'] = self.alert_config['email_username']
            msg['To'] = ', '.join(self.alert_config['email_recipients'])
            msg['Subject'] = f"[{alert['severity'].upper()}] Dataset Monitor Alert: {alert['type']}"
            
            body = f"""
Alert Type: {alert['type']}
Severity: {alert['severity']}
Message: {alert['message']}
Time: {datetime.now().isoformat()}

Details:
{json.dumps(alert.get('details', {}), indent=2)}
"""
            
            msg.attach(MIMEText(body, 'plain'))
            
            server = smtplib.SMTP(self.alert_config['email_smtp_server'], self.alert_config['email_smtp_port'])
            server.starttls()
            server.login(self.alert_config['email_username'], self.alert_config['email_password'])
            server.send_message(msg)
            server.quit()
            
            logger.info(f"Email alert sent for {alert['type']}")
            
        except Exception as e:
            logger.error(f"Error sending email alert: {e}")
    
    async def _send_webhook_alert(self, alert: Dict):
        """Send webhook alert"""
        try:
            import aiohttp
            
            payload = {
                'alert_type': alert['type'],
                'severity': alert['severity'],
                'message': alert['message'],
                'details': alert.get('details', {}),
                'timestamp': datetime.now().isoformat()
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(self.alert_config['webhook_url'], json=payload) as response:
                    if response.status == 200:
                        logger.info(f"Webhook alert sent for {alert['type']}")
                    else:
                        logger.error(f"Webhook alert failed: {response.status}")
            
        except Exception as e:
            logger.error(f"Error sending webhook alert: {e}")
    
    def get_system_status(self) -> Dict:
        """Get current system status"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get latest health status
            cursor.execute('''
                SELECT overall_health, metrics_summary, timestamp
                FROM system_health_history 
                ORDER BY timestamp DESC 
                LIMIT 1
            ''')
            
            result = cursor.fetchone()
            latest_health = {
                'overall_health': result[0] if result else 'unknown',
                'metrics': json.loads(result[1]) if result and result[1] else {},
                'last_check': result[2] if result else None
            }
            
            # Get recent alerts
            cursor.execute('''
                SELECT alert_type, severity, message, created_at, resolved
                FROM system_alerts 
                WHERE created_at > datetime('now', '-24 hours')
                ORDER BY created_at DESC
                LIMIT 10
            ''')
            
            recent_alerts = [
                {
                    'type': row[0],
                    'severity': row[1],
                    'message': row[2],
                    'created_at': row[3],
                    'resolved': bool(row[4])
                }
                for row in cursor.fetchall()
            ]
            
            conn.close()
            
            return {
                'health': latest_health,
                'recent_alerts': recent_alerts,
                'alert_thresholds': self.alert_thresholds,
                'monitoring_active': self.running
            }
            
        except Exception as e:
            logger.error(f"Error getting system status: {e}")
            return {'error': str(e)}

# CLI interface
async def main():
    """Main entry point for system monitor"""
    import argparse
    
    parser = argparse.ArgumentParser(description='System Monitor and Alerting')
    parser.add_argument('--mode', choices=['start', 'status'], default='start',
                       help='Operation mode')
    parser.add_argument('--interval', type=int, default=5,
                       help='Monitoring interval in minutes')
    parser.add_argument('--db', default='datasets.db',
                       help='Database file path')
    
    args = parser.parse_args()
    
    monitor = SystemMonitor(args.db)
    
    if args.mode == 'start':
        await monitor.start_monitoring(args.interval)
    elif args.mode == 'status':
        status = monitor.get_system_status()
        print(json.dumps(status, indent=2))

if __name__ == '__main__':
    asyncio.run(main())


