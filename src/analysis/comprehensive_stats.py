"""
Comprehensive Statistics Module for Data.gov Dataset Analysis
Provides cross-comparison and advanced analytics for all datasets
"""

import sqlite3
import json
import asyncio
import aiohttp
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
import logging
from collections import defaultdict, Counter
import statistics
import re

logger = logging.getLogger(__name__)

class ComprehensiveStats:
    """Comprehensive statistics and analytics for Data.gov datasets"""
    
    def __init__(self, db_path: str = "datasets.db"):
        self.db_path = db_path
        self.dataset_states_dir = Path("dataset_states")
        self.init_database()
    
    def init_database(self):
        """Initialize database tables for comprehensive stats"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create comprehensive stats tables
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dataset_analytics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id TEXT,
                agency TEXT,
                title TEXT,
                total_snapshots INTEGER,
                avg_row_count REAL,
                avg_column_count REAL,
                avg_file_size REAL,
                volatility_score REAL,
                availability_score REAL,
                change_count INTEGER DEFAULT 0,
                last_row_count INTEGER DEFAULT 0,
                last_column_count INTEGER DEFAULT 0,
                last_file_size INTEGER DEFAULT 0,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(dataset_id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS agency_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                agency TEXT UNIQUE,
                total_datasets INTEGER,
                avg_volatility REAL,
                avg_availability REAL,
                total_snapshots INTEGER,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS global_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stat_name TEXT UNIQUE,
                stat_value TEXT,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
    
    async def fetch_all_datagov_datasets(self) -> List[Dict]:
        """Fetch ALL datasets from Data.gov API with comprehensive metadata"""
        all_datasets = []
        offset = 0
        batch_size = 1000
        
        logger.info("Fetching all datasets from Data.gov API...")
        
        async with aiohttp.ClientSession() as session:
            while True:
                url = f"https://catalog.data.gov/api/3/action/package_search?rows={batch_size}&start={offset}&facet=true&facet.field=['organization','tags','res_format','groups']"
                
                try:
                    async with session.get(url, timeout=30) as response:
                        if response.status == 200:
                            data = await response.json()
                            datasets = data.get('result', {}).get('results', [])
                            
                            if not datasets:
                                break
                            
                            all_datasets.extend(datasets)
                            logger.info(f"Fetched {len(datasets)} datasets (total: {len(all_datasets)})")
                            
                            offset += batch_size
                            
                            # Rate limiting
                            await asyncio.sleep(0.1)
                        else:
                            logger.error(f"API error: {response.status}")
                            break
                
                except Exception as e:
                    logger.error(f"Error fetching datasets: {e}")
                    break
        
        logger.info(f"Total datasets fetched: {len(all_datasets)}")
        return all_datasets
    
    def process_dataset_metadata(self, dataset_id: str) -> Dict:
        """Process metadata for a single dataset from local files"""
        dataset_info = {
            'dataset_id': dataset_id,
            'title': 'Unknown',
            'agency': 'Unknown',
            'url': '',
            'description': '',
            'total_snapshots': 0,
            'snapshots': [],
            'availability_history': [],
            'size_history': [],
            'row_count_history': [],
            'column_count_history': []
        }
        
        dataset_dir = self.dataset_states_dir / dataset_id
        if not dataset_dir.exists():
            return dataset_info
        
        # Get all snapshot directories
        snapshot_dirs = [d for d in dataset_dir.iterdir() if d.is_dir()]
        dataset_info['total_snapshots'] = len(snapshot_dirs)
        
        for snapshot_dir in sorted(snapshot_dirs):
            metadata_file = snapshot_dir / 'metadata.json'
            if metadata_file.exists():
                try:
                    with open(metadata_file, 'r') as f:
                        metadata = json.load(f)
                        
                        # Update basic info from first snapshot
                        if not dataset_info['title'] or dataset_info['title'] == 'Unknown':
                            dataset_info.update({
                                'title': metadata.get('title', 'Unknown'),
                                'agency': metadata.get('agency', 'Unknown'),
                                'url': metadata.get('url', ''),
                                'description': metadata.get('metadata', {}).get('description', '')
                            })
                        
                        # Collect historical data
                        snapshot_data = {
                            'date': snapshot_dir.name,
                            'availability': metadata.get('availability', 'unknown'),
                            'row_count': metadata.get('row_count', 0),
                            'column_count': metadata.get('column_count', 0),
                            'file_size': metadata.get('file_size', 0),
                            'status_code': metadata.get('status_code', 0)
                        }
                        
                        dataset_info['snapshots'].append(snapshot_data)
                        dataset_info['availability_history'].append(metadata.get('availability', 'unknown'))
                        dataset_info['size_history'].append(metadata.get('file_size', 0))
                        dataset_info['row_count_history'].append(metadata.get('row_count', 0))
                        dataset_info['column_count_history'].append(metadata.get('column_count', 0))
                        
                except Exception as e:
                    logger.error(f"Error processing metadata for {dataset_id}/{snapshot_dir.name}: {e}")
        
        return dataset_info
    
    def process_dataset_from_db(self, dataset_id: str, title: str, agency: str, url: str) -> Dict:
        """Process dataset information directly from database records"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get all snapshots for this dataset
        cursor.execute('''
            SELECT snapshot_date, row_count, column_count, file_size, 
                   availability, status_code, created_at
            FROM dataset_states 
            WHERE dataset_id = ?
            ORDER BY snapshot_date ASC
        ''', (dataset_id,))
        
        snapshots = cursor.fetchall()
        conn.close()
        
        dataset_info = {
            'dataset_id': dataset_id,
            'title': title,
            'agency': agency,
            'url': url,
            'description': '',
            'total_snapshots': len(snapshots),
            'snapshots': [],
            'availability_history': [],
            'size_history': [],
            'row_count_history': [],
            'column_count_history': []
        }
        
        for snapshot in snapshots:
            snapshot_date, row_count, column_count, file_size, availability, status_code, created_at = snapshot
            
            snapshot_data = {
                'date': snapshot_date,
                'availability': availability or 'unknown',
                'row_count': row_count or 0,
                'column_count': column_count or 0,
                'file_size': file_size or 0,
                'status_code': status_code or 0
            }
            
            dataset_info['snapshots'].append(snapshot_data)
            dataset_info['availability_history'].append(availability or 'unknown')
            dataset_info['size_history'].append(file_size or 0)
            dataset_info['row_count_history'].append(row_count or 0)
            dataset_info['column_count_history'].append(column_count or 0)
        
        return dataset_info
    
    def calculate_dataset_analytics(self, dataset_info: Dict) -> Dict:
        """Calculate comprehensive analytics for a dataset"""
        analytics = {
            'dataset_id': dataset_info['dataset_id'],
            'title': dataset_info['title'],
            'agency': dataset_info['agency'],
            'total_snapshots': dataset_info['total_snapshots'],
            'avg_row_count': 0,
            'avg_column_count': 0,
            'avg_file_size': 0,
            'volatility_score': 0,
            'availability_score': 0,
            'change_count': 0,
            'last_row_count': 0,
            'last_column_count': 0,
            'last_file_size': 0,
            'trends': {},
            'anomalies': []
        }
        
        if not dataset_info['snapshots']:
            return analytics
        
        # Calculate averages
        row_counts = [s for s in dataset_info['row_count_history'] if s > 0]
        column_counts = [s for s in dataset_info['column_count_history'] if s > 0]
        file_sizes = [s for s in dataset_info['size_history'] if s > 0]
        
        # Get latest values
        if row_counts:
            analytics['last_row_count'] = row_counts[-1]
        if column_counts:
            analytics['last_column_count'] = column_counts[-1]
        if file_sizes:
            analytics['last_file_size'] = file_sizes[-1]
        
        analytics['avg_row_count'] = statistics.mean(row_counts) if row_counts else 0
        analytics['avg_column_count'] = statistics.mean(column_counts) if column_counts else 0
        analytics['avg_file_size'] = statistics.mean(file_sizes) if file_sizes else 0
        
        # Calculate change count by comparing consecutive snapshots
        change_count = 0
        snapshots = dataset_info['snapshots']
        
        for i in range(1, len(snapshots)):
            prev = snapshots[i-1]
            curr = snapshots[i]
            
            # Check for changes in key metrics
            if (prev['row_count'] != curr['row_count'] or 
                prev['column_count'] != curr['column_count'] or
                prev['file_size'] != curr['file_size'] or
                prev['availability'] != curr['availability']):
                change_count += 1
        
        analytics['change_count'] = change_count
        
        # Calculate volatility score (based on changes in size and structure)
        if len(dataset_info['snapshots']) > 1:
            size_changes = []
            row_changes = []
            col_changes = []
            
            for i in range(1, len(dataset_info['snapshots'])):
                prev = dataset_info['snapshots'][i-1]
                curr = dataset_info['snapshots'][i]
                
                if prev['file_size'] > 0 and curr['file_size'] > 0:
                    size_change = abs(curr['file_size'] - prev['file_size']) / prev['file_size']
                    size_changes.append(size_change)
                
                if prev['row_count'] > 0 and curr['row_count'] > 0:
                    row_change = abs(curr['row_count'] - prev['row_count']) / prev['row_count']
                    row_changes.append(row_change)
                
                if prev['column_count'] > 0 and curr['column_count'] > 0:
                    col_change = abs(curr['column_count'] - prev['column_count']) / prev['column_count']
                    col_changes.append(col_change)
            
            # Volatility score is average of all change percentages
            all_changes = size_changes + row_changes + col_changes
            analytics['volatility_score'] = statistics.mean(all_changes) if all_changes else 0
        
        # Calculate availability score
        availability_counts = Counter(dataset_info['availability_history'])
        total_snapshots = len(dataset_info['availability_history'])
        if total_snapshots > 0:
            analytics['availability_score'] = availability_counts.get('available', 0) / total_snapshots
        
        # Detect trends
        if len(dataset_info['snapshots']) >= 3:
            recent_snapshots = dataset_info['snapshots'][-3:]
            if all(s['row_count'] > 0 for s in recent_snapshots):
                row_trend = [s['row_count'] for s in recent_snapshots]
                if row_trend[0] < row_trend[1] < row_trend[2]:
                    analytics['trends']['row_count'] = 'increasing'
                elif row_trend[0] > row_trend[1] > row_trend[2]:
                    analytics['trends']['row_count'] = 'decreasing'
                else:
                    analytics['trends']['row_count'] = 'stable'
        
        # Detect anomalies
        if len(dataset_info['snapshots']) >= 2:
            for i, snapshot in enumerate(dataset_info['snapshots']):
                if i > 0:
                    prev = dataset_info['snapshots'][i-1]
                    
                    # Large size change
                    if prev['file_size'] > 0 and snapshot['file_size'] > 0:
                        size_change = abs(snapshot['file_size'] - prev['file_size']) / prev['file_size']
                        if size_change > 0.5:  # 50% change
                            analytics['anomalies'].append({
                                'date': snapshot['date'],
                                'type': 'size_change',
                                'change': f"{size_change:.1%}",
                                'from': prev['file_size'],
                                'to': snapshot['file_size']
                            })
                    
                    # Availability change
                    if prev['availability'] != snapshot['availability']:
                        analytics['anomalies'].append({
                            'date': snapshot['date'],
                            'type': 'availability_change',
                            'from': prev['availability'],
                            'to': snapshot['availability']
                        })
        
        return analytics
    
    def calculate_agency_stats(self, all_analytics: List[Dict]) -> Dict[str, Dict]:
        """Calculate statistics by agency"""
        agency_stats = defaultdict(lambda: {
            'datasets': [],
            'total_snapshots': 0,
            'avg_volatility': 0,
            'avg_availability': 0,
            'total_datasets': 0
        })
        
        for analytics in all_analytics:
            agency = analytics['agency']
            agency_stats[agency]['datasets'].append(analytics)
            agency_stats[agency]['total_datasets'] += 1
            agency_stats[agency]['total_snapshots'] += analytics['total_snapshots']
        
        # Calculate averages
        for agency, stats in agency_stats.items():
            if stats['datasets']:
                volatilities = [d['volatility_score'] for d in stats['datasets'] if d['volatility_score'] > 0]
                availabilities = [d['availability_score'] for d in stats['datasets'] if d['availability_score'] > 0]
                
                stats['avg_volatility'] = statistics.mean(volatilities) if volatilities else 0
                stats['avg_availability'] = statistics.mean(availabilities) if availabilities else 0
        
        return dict(agency_stats)
    
    def calculate_global_stats(self, all_analytics: List[Dict]) -> Dict:
        """Calculate global statistics across all datasets"""
        if not all_analytics:
            return {}
        
        total_datasets = len(all_analytics)
        total_snapshots = sum(d['total_snapshots'] for d in all_analytics)
        
        # Availability distribution
        availability_scores = [d['availability_score'] for d in all_analytics]
        avg_availability = statistics.mean(availability_scores) if availability_scores else 0
        
        # Volatility distribution
        volatility_scores = [d['volatility_score'] for d in all_analytics if d['volatility_score'] > 0]
        avg_volatility = statistics.mean(volatility_scores) if volatility_scores else 0
        
        # Size distribution
        avg_sizes = [d['avg_file_size'] for d in all_analytics if d['avg_file_size'] > 0]
        median_size = statistics.median(avg_sizes) if avg_sizes else 0
        
        # Most volatile datasets
        most_volatile = sorted(all_analytics, key=lambda x: x['volatility_score'], reverse=True)[:10]
        
        # Most stable datasets
        most_stable = sorted([d for d in all_analytics if d['volatility_score'] > 0], 
                           key=lambda x: x['volatility_score'])[:10]
        
        # Agency distribution
        agency_counts = Counter(d['agency'] for d in all_analytics)
        top_agencies = agency_counts.most_common(10)
        
        return {
            'total_datasets': total_datasets,
            'total_snapshots': total_snapshots,
            'avg_availability': avg_availability,
            'avg_volatility': avg_volatility,
            'median_file_size': median_size,
            'most_volatile_datasets': most_volatile,
            'most_stable_datasets': most_stable,
            'top_agencies': top_agencies,
            'last_updated': datetime.now().isoformat()
        }
    
    def store_analytics(self, analytics: List[Dict], agency_stats: Dict, global_stats: Dict):
        """Store calculated analytics in database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Store dataset analytics
            for analytics_data in analytics:
                cursor.execute('''
                    INSERT OR REPLACE INTO dataset_analytics
                    (dataset_id, agency, title, total_snapshots, avg_row_count, 
                     avg_column_count, avg_file_size, volatility_score, availability_score,
                     change_count, last_row_count, last_column_count, last_file_size)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    analytics_data['dataset_id'],
                    analytics_data['agency'],
                    analytics_data['title'],
                    analytics_data['total_snapshots'],
                    analytics_data['avg_row_count'],
                    analytics_data['avg_column_count'],
                    analytics_data['avg_file_size'],
                    analytics_data['volatility_score'],
                    analytics_data['availability_score'],
                    analytics_data['change_count'],
                    analytics_data['last_row_count'],
                    analytics_data['last_column_count'],
                    analytics_data['last_file_size']
                ))
            
            # Store agency stats
            for agency, stats in agency_stats.items():
                cursor.execute('''
                    INSERT OR REPLACE INTO agency_stats
                    (agency, total_datasets, avg_volatility, avg_availability, total_snapshots)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    agency,
                    stats['total_datasets'],
                    stats['avg_volatility'],
                    stats['avg_availability'],
                    stats['total_snapshots']
                ))
            
            # Store global stats
            for key, value in global_stats.items():
                if isinstance(value, (dict, list)):
                    value = json.dumps(value)
                cursor.execute('''
                    INSERT OR REPLACE INTO global_stats
                    (stat_name, stat_value)
                    VALUES (?, ?)
                ''', (key, str(value)))
            
            conn.commit()
            logger.info("Analytics stored successfully")
            
        except Exception as e:
            logger.error(f"Error storing analytics: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    async def run_comprehensive_analysis(self, incremental=False) -> Dict:
        """Run comprehensive analysis of all datasets or just new ones"""
        logger.info(f"Starting {'incremental' if incremental else 'comprehensive'} dataset analysis...")
        start_time = datetime.now()
        
        # Get all dataset IDs from database
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        if incremental:
            # Only process datasets not yet in analytics
            cursor.execute('''
                SELECT DISTINCT ds.dataset_id, ds.title, ds.agency, ds.url
                FROM dataset_states ds
                LEFT JOIN dataset_analytics da ON ds.dataset_id = da.dataset_id
                WHERE ds.title IS NOT NULL AND ds.title != '' AND ds.title != 'Unknown'
                AND da.dataset_id IS NULL
                ORDER BY ds.dataset_id
            ''')
        else:
            # Process all datasets
            cursor.execute('''
                SELECT DISTINCT dataset_id, title, agency, url
                FROM dataset_states 
                WHERE title IS NOT NULL AND title != '' AND title != 'Unknown'
                ORDER BY dataset_id
            ''')
        
        dataset_records = cursor.fetchall()
        conn.close()
        
        logger.info(f"Found {len(dataset_records)} datasets to process")
        
        # Process each dataset
        all_analytics = []
        for i, (dataset_id, title, agency, url) in enumerate(dataset_records):
            if i % 100 == 0:
                logger.info(f"Processing dataset {i+1}/{len(dataset_records)}")
            
            dataset_info = self.process_dataset_from_db(dataset_id, title, agency, url)
            analytics = self.calculate_dataset_analytics(dataset_info)
            all_analytics.append(analytics)
        
        # Calculate agency and global stats
        agency_stats = self.calculate_agency_stats(all_analytics)
        global_stats = self.calculate_global_stats(all_analytics)
        
        # Store results
        self.store_analytics(all_analytics, agency_stats, global_stats)
        
        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"Comprehensive analysis completed in {duration:.2f} seconds")
        
        return {
            'total_datasets_analyzed': len(all_analytics),
            'agency_stats': agency_stats,
            'global_stats': global_stats,
            'duration_seconds': duration,
            'timestamp': datetime.now().isoformat()
        }
    
    def get_dashboard_stats(self) -> Dict:
        """Get statistics formatted for dashboard display"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Get global stats
            cursor.execute('SELECT stat_name, stat_value FROM global_stats')
            global_stats = {row[0]: row[1] for row in cursor.fetchall()}
            
            # Get dataset counts
            cursor.execute('SELECT COUNT(*) FROM dataset_analytics')
            total_datasets = cursor.fetchone()[0] or 0
            
            cursor.execute('SELECT SUM(total_snapshots) FROM dataset_analytics')
            total_snapshots = cursor.fetchone()[0] or 0
            
            # Get availability distribution
            cursor.execute('''
                SELECT 
                    SUM(CASE WHEN availability_score >= 0.8 THEN 1 ELSE 0 END) as available,
                    SUM(CASE WHEN availability_score < 0.8 AND availability_score >= 0.5 THEN 1 ELSE 0 END) as partially_available,
                    SUM(CASE WHEN availability_score < 0.5 THEN 1 ELSE 0 END) as unavailable
                FROM dataset_analytics
            ''')
            availability_row = cursor.fetchone()
            availability_stats = {
                'available': availability_row[0] or 0,
                'partially_available': availability_row[1] or 0,
                'unavailable': availability_row[2] or 0
            }
            
            # Get top agencies
            cursor.execute('''
                SELECT agency, total_datasets, avg_availability
                FROM agency_stats
                ORDER BY total_datasets DESC
                LIMIT 10
            ''')
            top_agencies = [{'agency': row[0], 'datasets': row[1], 'availability': row[2]} 
                           for row in cursor.fetchall()]
            
            # Get most volatile datasets
            cursor.execute('''
                SELECT dataset_id, title, agency, volatility_score
                FROM dataset_analytics
                WHERE volatility_score > 0
                ORDER BY volatility_score DESC
                LIMIT 10
            ''')
            most_volatile = [{'dataset_id': row[0], 'title': row[1], 'agency': row[2], 'volatility': row[3]} 
                           for row in cursor.fetchall()]
            
            return {
                'total_datasets': total_datasets,
                'total_snapshots': total_snapshots,
                'availability_stats': availability_stats,
                'top_agencies': top_agencies,
                'most_volatile_datasets': most_volatile,
                'global_stats': global_stats,
                'last_updated': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting dashboard stats: {e}")
            return {'error': str(e)}
        finally:
            conn.close()
    
    def get_agency_comparison(self) -> List[Dict]:
        """Get agency comparison data for cross-analysis"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT agency, total_datasets, avg_volatility, avg_availability, total_snapshots
                FROM agency_stats
                ORDER BY total_datasets DESC
            ''')
            
            agencies = []
            for row in cursor.fetchall():
                agencies.append({
                    'agency': row[0],
                    'total_datasets': row[1],
                    'avg_volatility': row[2],
                    'avg_availability': row[3],
                    'total_snapshots': row[4]
                })
            
            return agencies
            
        except Exception as e:
            logger.error(f"Error getting agency comparison: {e}")
            return []
        finally:
            conn.close()
    
    def get_dataset_trends(self, limit: int = 50) -> List[Dict]:
        """Get dataset trends for analysis"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT dataset_id, title, agency, volatility_score, availability_score, 
                       avg_file_size, total_snapshots
                FROM dataset_analytics
                ORDER BY volatility_score DESC
                LIMIT ?
            ''', (limit,))
            
            trends = []
            for row in cursor.fetchall():
                trends.append({
                    'dataset_id': row[0],
                    'title': row[1],
                    'agency': row[2],
                    'volatility_score': row[3],
                    'availability_score': row[4],
                    'avg_file_size': row[5],
                    'total_snapshots': row[6]
                })
            
            return trends
            
        except Exception as e:
            logger.error(f"Error getting dataset trends: {e}")
            return []
        finally:
            conn.close()
    
    def process_new_datasets(self):
        """Process only new datasets that haven't been analyzed yet"""
        logger.info("Processing new datasets...")
        
        # Run incremental analysis
        import asyncio
        return asyncio.run(self.run_comprehensive_analysis(incremental=True))
    
    def update_existing_dataset(self, dataset_id: str):
        """Update analytics for a specific dataset"""
        logger.info(f"Updating analytics for dataset {dataset_id}")
        
        # Get dataset info from database
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT DISTINCT dataset_id, title, agency, url
            FROM dataset_states 
            WHERE dataset_id = ? AND title IS NOT NULL AND title != '' AND title != 'Unknown'
        ''', (dataset_id,))
        
        result = cursor.fetchone()
        conn.close()
        
        if not result:
            logger.warning(f"Dataset {dataset_id} not found or has no valid title")
            return None
        
        dataset_id, title, agency, url = result
        
        # Process the dataset
        dataset_info = self.process_dataset_from_db(dataset_id, title, agency, url)
        analytics = self.calculate_dataset_analytics(dataset_info)
        
        # Store updated analytics
        self.store_analytics([analytics], {}, {})
        
        logger.info(f"Updated analytics for dataset {dataset_id}")
        return analytics
