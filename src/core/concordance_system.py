"""
Concordance: Dataset State Historian - Main System
Integrates all components for comprehensive dataset state tracking
"""

import logging
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from pathlib import Path

from src.core.data_fetcher import DataFetcher
from src.core.state_historian import DatasetStateHistorian
from src.analysis.state_explorer import StateExplorer
from src.analysis.post_mortem_analyzer import PostMortemAnalyzer
from src.analysis.baseline_manager import BaselineManager

logger = logging.getLogger(__name__)

class ConcordanceSystem:
    def __init__(self, api_key: Optional[str] = None, db_path: str = "datasets.db"):
        self.data_fetcher = DataFetcher(db_path, api_key)
        self.state_historian = DatasetStateHistorian(db_path=db_path)
        self.state_explorer = StateExplorer(db_path=db_path)
        self.postmortem_analyzer = PostMortemAnalyzer(db_path=db_path)
        self.baseline_manager = BaselineManager(db_path=db_path)
        
        # Create output directories
        Path("dataset_states").mkdir(exist_ok=True)
        Path("state_reports").mkdir(exist_ok=True)
        Path("postmortems").mkdir(exist_ok=True)
    
    def run_full_analysis(self, target_datasets: Optional[List[str]] = None, 
                         max_datasets: int = 10) -> Dict:
        """Run complete analysis pipeline"""
        logger.info("Starting Concordance full analysis")
        
        # Step 1: Fetch current datasets
        logger.info("Step 1: Fetching current datasets")
        current_datasets = self.data_fetcher.fetch_live_datagov_catalog()
        
        if not current_datasets:
            logger.error("No datasets fetched - cannot proceed")
            return {'error': 'No datasets available'}
        
        # Filter to target datasets if specified
        if target_datasets:
            current_datasets = [d for d in current_datasets if d.get('id') in target_datasets]
        
        # Limit to max_datasets
        current_datasets = current_datasets[:max_datasets]
        
        logger.info(f"Analyzing {len(current_datasets)} datasets")
        
        # Step 2: Create state snapshots
        logger.info("Step 2: Creating state snapshots")
        snapshot_results = []
        for i, dataset in enumerate(current_datasets):
            logger.info(f"Snapshotting dataset {i+1}/{len(current_datasets)}: {dataset.get('id', 'unknown')}")
            try:
                state_info = self.state_historian.snapshot_dataset_state(dataset)
                snapshot_results.append({
                    'dataset_id': dataset.get('id'),
                    'status': 'success',
                    'state_info': state_info
                })
            except Exception as e:
                logger.error(f"Failed to snapshot {dataset.get('id')}: {e}")
                snapshot_results.append({
                    'dataset_id': dataset.get('id'),
                    'status': 'error',
                    'error': str(e)
                })
        
        # Step 3: Compare with baseline
        logger.info("Step 3: Comparing with baseline")
        comparison_result = self.baseline_manager.compare_with_baseline(current_datasets)
        
        # Step 4: Generate reports
        logger.info("Step 4: Generating reports")
        report_results = self._generate_all_reports(current_datasets, comparison_result)
        
        # Step 5: Analyze vanished datasets
        logger.info("Step 5: Analyzing vanished datasets")
        postmortem_results = self._analyze_vanished_datasets(comparison_result.get('vanished', []))
        
        return {
            'timestamp': datetime.now().isoformat(),
            'datasets_analyzed': len(current_datasets),
            'snapshot_results': snapshot_results,
            'comparison_result': comparison_result,
            'report_results': report_results,
            'postmortem_results': postmortem_results,
            'summary': self._generate_summary(comparison_result, postmortem_results)
        }
    
    def _generate_all_reports(self, datasets: List[Dict], comparison_result: Dict) -> Dict:
        """Generate all types of reports"""
        report_results = {
            'individual_reports': [],
            'summary_report': None,
            'vanished_reports': []
        }
        
        # Generate individual dataset reports
        for dataset in datasets:
            dataset_id = dataset.get('id')
            if dataset_id:
                try:
                    report_path = self.state_explorer.generate_dataset_report(dataset_id)
                    report_results['individual_reports'].append({
                        'dataset_id': dataset_id,
                        'report_path': report_path,
                        'status': 'success'
                    })
                except Exception as e:
                    logger.error(f"Failed to generate report for {dataset_id}: {e}")
                    report_results['individual_reports'].append({
                        'dataset_id': dataset_id,
                        'status': 'error',
                        'error': str(e)
                    })
        
        # Generate summary report
        try:
            summary_report_path = self.state_explorer.generate_summary_report()
            report_results['summary_report'] = {
                'report_path': summary_report_path,
                'status': 'success'
            }
        except Exception as e:
            logger.error(f"Failed to generate summary report: {e}")
            report_results['summary_report'] = {
                'status': 'error',
                'error': str(e)
            }
        
        return report_results
    
    def _analyze_vanished_datasets(self, vanished_datasets: List[Dict]) -> Dict:
        """Analyze vanished datasets for post-mortem reports"""
        postmortem_results = {
            'analyzed': [],
            'errors': []
        }
        
        for dataset in vanished_datasets:
            dataset_id = dataset.get('id')
            if dataset_id:
                try:
                    postmortem = self.postmortem_analyzer.analyze_vanished_dataset(dataset_id)
                    report_path = self.postmortem_analyzer.generate_postmortem_report(dataset_id)
                    
                    postmortem_results['analyzed'].append({
                        'dataset_id': dataset_id,
                        'postmortem': postmortem,
                        'report_path': report_path,
                        'status': 'success'
                    })
                except Exception as e:
                    logger.error(f"Failed to analyze vanished dataset {dataset_id}: {e}")
                    postmortem_results['errors'].append({
                        'dataset_id': dataset_id,
                        'error': str(e)
                    })
        
        return postmortem_results
    
    def _generate_summary(self, comparison_result: Dict, postmortem_results: Dict) -> Dict:
        """Generate analysis summary"""
        vanished_count = len(comparison_result.get('vanished', []))
        new_count = len(comparison_result.get('new', []))
        changed_count = len(comparison_result.get('changed', []))
        
        analyzed_vanished = len(postmortem_results.get('analyzed', []))
        postmortem_errors = len(postmortem_results.get('errors', []))
        
        return {
            'vanished_datasets': vanished_count,
            'new_datasets': new_count,
            'changed_datasets': changed_count,
            'postmortems_generated': analyzed_vanished,
            'postmortem_errors': postmortem_errors,
            'analysis_timestamp': datetime.now().isoformat()
        }
    
    def get_system_status(self) -> Dict:
        """Get comprehensive system status"""
        # Get baseline stats
        baseline_stats = self.baseline_manager.get_system_stats()
        
        # Get API connectivity
        api_stats = self.data_fetcher.get_api_stats()
        
        # Get state historian stats
        state_stats = self._get_state_historian_stats()
        
        return {
            'timestamp': datetime.now().isoformat(),
            'baseline_stats': baseline_stats,
            'api_stats': api_stats,
            'state_stats': state_stats,
            'system_health': self._assess_system_health(baseline_stats, api_stats, state_stats)
        }
    
    def _get_state_historian_stats(self) -> Dict:
        """Get state historian statistics"""
        conn = self.state_historian.db_path
        import sqlite3
        
        try:
            conn = sqlite3.connect(conn)
            cursor = conn.cursor()
            
            # Get snapshot counts
            cursor.execute('SELECT COUNT(*) FROM dataset_states')
            total_snapshots = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(DISTINCT dataset_id) FROM dataset_states')
            unique_datasets = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM state_diffs')
            total_diffs = cursor.fetchone()[0]
            
            conn.close()
            
            return {
                'total_snapshots': total_snapshots,
                'unique_datasets': unique_datasets,
                'total_diffs': total_diffs
            }
        except Exception as e:
            logger.error(f"Failed to get state historian stats: {e}")
            return {'error': str(e)}
    
    def _assess_system_health(self, baseline_stats: Dict, api_stats: Dict, state_stats: Dict) -> str:
        """Assess overall system health"""
        health_score = 0
        
        # Check baseline
        if baseline_stats.get('baseline_snapshots', 0) > 0:
            health_score += 1
        
        # Check API
        if api_stats.get('datagov_available', False):
            health_score += 1
        
        # Check state tracking
        if state_stats.get('total_snapshots', 0) > 0:
            health_score += 1
        
        if health_score == 3:
            return 'excellent'
        elif health_score == 2:
            return 'good'
        elif health_score == 1:
            return 'fair'
        else:
            return 'poor'
    
    def run_minimal_mvp(self, target_datasets: List[str]) -> Dict:
        """Run minimal MVP for specific datasets"""
        logger.info(f"Running minimal MVP for {len(target_datasets)} datasets")
        
        # Run full analysis on target datasets
        result = self.run_full_analysis(target_datasets=target_datasets, max_datasets=len(target_datasets))
        
        # Generate MVP summary
        mvp_summary = {
            'mvp_datasets': target_datasets,
            'analysis_result': result,
            'mvp_timestamp': datetime.now().isoformat(),
            'next_steps': [
                "Review individual dataset reports in state_reports/",
                "Check post-mortem analyses in postmortems/",
                "Examine state snapshots in dataset_states/",
                "Run full analysis on more datasets to scale up"
            ]
        }
        
        # Save MVP results
        mvp_file = Path("mvp_results.json")
        with open(mvp_file, 'w') as f:
            json.dump(mvp_summary, f, indent=2, default=str)
        
        logger.info(f"MVP results saved to {mvp_file}")
        return mvp_summary

def main():
    """Main entry point for Concordance system"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Concordance: Dataset State Historian')
    parser.add_argument('--mode', choices=['full', 'mvp', 'status'], default='status',
                       help='Analysis mode')
    parser.add_argument('--datasets', nargs='+', help='Target dataset IDs for MVP mode')
    parser.add_argument('--api-key', help='Data.gov API key')
    parser.add_argument('--max-datasets', type=int, default=10, help='Maximum datasets to analyze')
    
    args = parser.parse_args()
    
    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Initialize system
    system = ConcordanceSystem(api_key=args.api_key)
    
    if args.mode == 'status':
        status = system.get_system_status()
        print(json.dumps(status, indent=2, default=str))
    
    elif args.mode == 'mvp':
        if not args.datasets:
            print("Error: --datasets required for MVP mode")
            return
        
        result = system.run_minimal_mvp(args.datasets)
        print(json.dumps(result, indent=2, default=str))
    
    elif args.mode == 'full':
        result = system.run_full_analysis(max_datasets=args.max_datasets)
        print(json.dumps(result, indent=2, default=str))

if __name__ == '__main__':
    main()
