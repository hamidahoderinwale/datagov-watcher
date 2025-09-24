"""
Backfill Dimensions for Existing Datasets
Processes all existing datasets to ensure they have row/column counts computed
"""

import asyncio
import sqlite3
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional
from processing.enhanced_row_column_computer import EnhancedRowColumnComputer

logger = logging.getLogger(__name__)

class DimensionBackfillProcessor:
    """Processes existing datasets to backfill missing dimension data"""
    
    def __init__(self, db_path: str = "datasets.db"):
        self.db_path = db_path
        self.computer = EnhancedRowColumnComputer(db_path)
        
    async def backfill_all_missing_dimensions(self, force_recompute: bool = False) -> Dict:
        """Backfill dimensions for all datasets missing this data"""
        logger.info("Starting backfill process for missing dimensions")
        
        results = {
            'start_time': datetime.now().isoformat(),
            'datasets_processed': 0,
            'datasets_updated': 0,
            'datasets_failed': 0,
            'datasets_skipped': 0,
            'errors': []
        }
        
        try:
            # Get datasets that need dimension computation
            datasets_to_process = self.get_datasets_needing_backfill(force_recompute)
            logger.info(f"Found {len(datasets_to_process)} datasets needing dimension backfill")
            
            if not datasets_to_process:
                logger.info("No datasets need dimension backfill")
                return results
            
            # Process datasets using the enhanced computer
            computer_results = await self.computer.ensure_all_datasets_have_dimensions(force_recompute)
            
            # Update our results
            results.update(computer_results)
            
            # Generate backfill report
            self.generate_backfill_report(results)
            
            logger.info(f"Backfill complete: {results['datasets_updated']} updated, {results['datasets_failed']} failed")
            return results
            
        except Exception as e:
            logger.error(f"Error in backfill process: {e}")
            results['errors'].append(f"Backfill error: {str(e)}")
            results['end_time'] = datetime.now().isoformat()
            return results
    
    def get_datasets_needing_backfill(self, force_recompute: bool = False) -> List[Dict]:
        """Get datasets that need dimension backfill"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        if force_recompute:
            # Get all available datasets
            query = '''
                SELECT DISTINCT ds.dataset_id, ds.title, ds.agency, ds.url, ds.resource_format, ds.availability
                FROM dataset_states ds
                INNER JOIN (
                    SELECT dataset_id, MAX(created_at) as max_created
                    FROM dataset_states 
                    GROUP BY dataset_id
                ) latest ON ds.dataset_id = latest.dataset_id 
                AND ds.created_at = latest.max_created
                WHERE ds.url IS NOT NULL 
                AND ds.url != ''
                AND ds.availability = 'available'
                ORDER BY ds.created_at DESC
            '''
        else:
            # Get datasets missing dimension data
            query = '''
                SELECT DISTINCT ds.dataset_id, ds.title, ds.agency, ds.url, ds.resource_format, ds.availability
                FROM dataset_states ds
                INNER JOIN (
                    SELECT dataset_id, MAX(created_at) as max_created
                    FROM dataset_states 
                    GROUP BY dataset_id
                ) latest ON ds.dataset_id = latest.dataset_id 
                AND ds.created_at = latest.max_created
                WHERE ds.url IS NOT NULL 
                AND ds.url != ''
                AND ds.availability = 'available'
                AND (ds.row_count IS NULL OR ds.row_count = 0 OR ds.column_count IS NULL OR ds.column_count = 0)
                ORDER BY ds.created_at DESC
            '''
        
        cursor.execute(query)
        datasets = []
        
        for row in cursor.fetchall():
            datasets.append({
                'dataset_id': row[0],
                'title': row[1],
                'agency': row[2],
                'url': row[3],
                'resource_format': row[4] or 'CSV',
                'availability': row[5]
            })
        
        conn.close()
        return datasets
    
    def generate_backfill_report(self, results: Dict):
        """Generate a backfill report"""
        try:
            report = {
                'backfill_summary': {
                    'start_time': results['start_time'],
                    'end_time': results.get('end_time', datetime.now().isoformat()),
                    'datasets_processed': results['datasets_processed'],
                    'datasets_updated': results['datasets_updated'],
                    'datasets_failed': results['datasets_failed'],
                    'datasets_skipped': results['datasets_skipped'],
                    'success_rate': (results['datasets_updated'] / results['datasets_processed'] * 100) if results['datasets_processed'] > 0 else 0
                },
                'errors': results['errors'][:10],  # First 10 errors
                'total_errors': len(results['errors'])
            }
            
            # Save report
            report_path = f"backfill_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(report_path, 'w') as f:
                json.dump(report, f, indent=2)
            
            logger.info(f"Backfill report saved to {report_path}")
            
        except Exception as e:
            logger.error(f"Error generating backfill report: {e}")
    
    def get_backfill_statistics(self) -> Dict:
        """Get statistics about datasets needing backfill"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Get total datasets
            cursor.execute('SELECT COUNT(DISTINCT dataset_id) FROM dataset_states')
            total_datasets = cursor.fetchone()[0] or 0
            
            # Get datasets with dimensions
            cursor.execute('''
                SELECT COUNT(DISTINCT dataset_id) 
                FROM dataset_states 
                WHERE row_count IS NOT NULL AND row_count > 0 
                AND column_count IS NOT NULL AND column_count > 0
            ''')
            datasets_with_dimensions = cursor.fetchone()[0] or 0
            
            # Get datasets missing dimensions
            cursor.execute('''
                SELECT COUNT(DISTINCT dataset_id) 
                FROM dataset_states ds
                INNER JOIN (
                    SELECT dataset_id, MAX(created_at) as max_created
                    FROM dataset_states 
                    GROUP BY dataset_id
                ) latest ON ds.dataset_id = latest.dataset_id 
                AND ds.created_at = latest.max_created
                WHERE (ds.row_count IS NULL OR ds.row_count = 0 OR ds.column_count IS NULL OR ds.column_count = 0)
                AND ds.availability = 'available'
            ''')
            datasets_missing_dimensions = cursor.fetchone()[0] or 0
            
            # Get datasets by agency
            cursor.execute('''
                SELECT ds.agency, COUNT(DISTINCT ds.dataset_id) as total,
                       COUNT(CASE WHEN ds.row_count > 0 AND ds.column_count > 0 THEN 1 END) as with_dimensions
                FROM dataset_states ds
                INNER JOIN (
                    SELECT dataset_id, MAX(created_at) as max_created
                    FROM dataset_states 
                    GROUP BY dataset_id
                ) latest ON ds.dataset_id = latest.dataset_id 
                AND ds.created_at = latest.max_created
                WHERE ds.availability = 'available'
                GROUP BY ds.agency
                ORDER BY total DESC
                LIMIT 20
            ''')
            
            agency_stats = []
            for row in cursor.fetchall():
                agency, total, with_dimensions = row
                completion_rate = (with_dimensions / total * 100) if total > 0 else 0
                agency_stats.append({
                    'agency': agency or 'Unknown',
                    'total_datasets': total,
                    'with_dimensions': with_dimensions,
                    'completion_rate': round(completion_rate, 2)
                })
            
            return {
                'total_datasets': total_datasets,
                'datasets_with_dimensions': datasets_with_dimensions,
                'datasets_missing_dimensions': datasets_missing_dimensions,
                'completion_percentage': (datasets_with_dimensions / total_datasets * 100) if total_datasets > 0 else 0,
                'agency_statistics': agency_stats
            }
            
        except Exception as e:
            logger.error(f"Error getting backfill statistics: {e}")
            return {}
        finally:
            conn.close()
    
    def validate_dimension_data(self) -> Dict:
        """Validate the quality of dimension data"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Get datasets with dimensions
            cursor.execute('''
                SELECT dataset_id, row_count, column_count, resource_format
                FROM dataset_states ds
                INNER JOIN (
                    SELECT dataset_id, MAX(created_at) as max_created
                    FROM dataset_states 
                    GROUP BY dataset_id
                ) latest ON ds.dataset_id = latest.dataset_id 
                AND ds.created_at = latest.max_created
                WHERE ds.row_count IS NOT NULL AND ds.row_count > 0 
                AND ds.column_count IS NOT NULL AND ds.column_count > 0
            ''')
            
            validation_results = {
                'total_validated': 0,
                'valid_dimensions': 0,
                'invalid_dimensions': 0,
                'issues': []
            }
            
            for row in cursor.fetchall():
                dataset_id, row_count, column_count, resource_format = row
                validation_results['total_validated'] += 1
                
                # Check for reasonable dimensions
                if row_count > 0 and column_count > 0 and row_count < 10000000 and column_count < 1000:
                    validation_results['valid_dimensions'] += 1
                else:
                    validation_results['invalid_dimensions'] += 1
                    validation_results['issues'].append({
                        'dataset_id': dataset_id,
                        'row_count': row_count,
                        'column_count': column_count,
                        'resource_format': resource_format,
                        'issue': 'Unreasonable dimensions'
                    })
            
            return validation_results
            
        except Exception as e:
            logger.error(f"Error validating dimension data: {e}")
            return {}
        finally:
            conn.close()

async def main():
    """Main function for running backfill"""
    processor = DimensionBackfillProcessor()
    
    # Get current statistics
    stats = processor.get_backfill_statistics()
    print("Current backfill statistics:")
    print(json.dumps(stats, indent=2))
    
    # Run backfill
    results = await processor.backfill_all_missing_dimensions(force_recompute=False)
    print("\nBackfill results:")
    print(json.dumps(results, indent=2))
    
    # Validate results
    validation = processor.validate_dimension_data()
    print("\nValidation results:")
    print(json.dumps(validation, indent=2))

if __name__ == '__main__':
    asyncio.run(main())


