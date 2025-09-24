#!/usr/bin/env python3
"""
Ensure Dimensions Script
Comprehensive script to ensure all datasets have row/column counts computed
"""

import asyncio
import argparse
import json
import logging
from datetime import datetime
from processing.enhanced_row_column_computer import EnhancedRowColumnComputer
from processing.backfill_dimensions import DimensionBackfillProcessor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('dimension_computation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

async def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Ensure all datasets have row/column dimensions')
    parser.add_argument('--mode', choices=['compute', 'backfill', 'validate', 'stats'], 
                       default='compute', help='Operation mode')
    parser.add_argument('--force', action='store_true', 
                       help='Force recomputation of existing dimensions')
    parser.add_argument('--limit', type=int, default=None,
                       help='Limit number of datasets to process')
    parser.add_argument('--db', default='datasets.db',
                       help='Database file path')
    
    args = parser.parse_args()
    
    # Initialize processors
    computer = EnhancedRowColumnComputer(args.db)
    backfill = DimensionBackfillProcessor(args.db)
    
    print(f"🧬 Dataset State Historian - Dimension Computation")
    print(f"📊 Mode: {args.mode}")
    print(f"🔧 Database: {args.db}")
    print(f"⚡ Force recompute: {args.force}")
    print("-" * 60)
    
    if args.mode == 'stats':
        # Show current statistics
        print("📈 Current Dimension Statistics:")
        stats = computer.get_dimension_statistics()
        print(json.dumps(stats, indent=2))
        
        print("\n📊 Backfill Statistics:")
        backfill_stats = backfill.get_backfill_statistics()
        print(json.dumps(backfill_stats, indent=2))
        
    elif args.mode == 'validate':
        # Validate existing dimension data
        print("🔍 Validating Dimension Data Quality...")
        validation = backfill.validate_dimension_data()
        print(json.dumps(validation, indent=2))
        
    elif args.mode == 'backfill':
        # Run backfill process
        print("🔄 Starting Backfill Process...")
        results = await backfill.backfill_all_missing_dimensions(args.force)
        
        print("\n📊 Backfill Results:")
        print(f"✅ Datasets processed: {results['datasets_processed']}")
        print(f"✅ Datasets updated: {results['datasets_updated']}")
        print(f"❌ Datasets failed: {results['datasets_failed']}")
        print(f"⏭️  Datasets skipped: {results['datasets_skipped']}")
        
        if results['errors']:
            print(f"\n⚠️  Errors ({len(results['errors'])}):")
            for error in results['errors'][:10]:  # Show first 10 errors
                print(f"   - {error}")
        
    elif args.mode == 'compute':
        # Run comprehensive computation
        print("🚀 Starting Comprehensive Dimension Computation...")
        results = await computer.ensure_all_datasets_have_dimensions(args.force)
        
        print("\n📊 Computation Results:")
        print(f"✅ Datasets processed: {results['datasets_processed']}")
        print(f"✅ Datasets updated: {results['datasets_updated']}")
        print(f"❌ Datasets failed: {results['datasets_failed']}")
        print(f"⏭️  Datasets skipped: {results['datasets_skipped']}")
        
        if results['errors']:
            print(f"\n⚠️  Errors ({len(results['errors'])}):")
            for error in results['errors'][:10]:  # Show first 10 errors
                print(f"   - {error}")
    
    print("\n🎉 Process completed!")
    print(f"⏰ Finished at: {datetime.now().isoformat()}")

if __name__ == '__main__':
    asyncio.run(main())


