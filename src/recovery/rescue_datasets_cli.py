#!/usr/bin/env python3
"""
Dataset Recovery Command-Line Utility
Based on University of Michigan "Missing Government Websites and Data" guide
"""

import argparse
import json
import sys
import yaml
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from recovery.unified_recovery_system import UnifiedRecoverySystem, DatasetMetadata, RecoveryStatus

class RescueDatasetsCLI:
    """Command-line interface for dataset recovery operations"""
    
    def __init__(self, config_path: Optional[str] = None):
        self.config = self.load_config(config_path)
        self.recovery_system = UnifiedRecoverySystem()
        
    def load_config(self, config_path: Optional[str]) -> Dict:
        """Load configuration from YAML file"""
        default_config = {
            'output_dir': './rescued_datasets',
            'log_level': 'INFO',
            'max_concurrent_searches': 5,
            'timeout_seconds': 30,
            'sources': {
                'lil': {'enabled': True, 'priority': 1},
                'findlostdata': {'enabled': True, 'priority': 2},
                'datalumos': {'enabled': True, 'priority': 3},
                'wayback': {'enabled': True, 'priority': 4},
                'edgi': {'enabled': True, 'priority': 5},
                'eot': {'enabled': True, 'priority': 6},
                'webrecorder': {'enabled': True, 'priority': 7},
                'ucsb': {'enabled': True, 'priority': 8}
            }
        }
        
        if config_path and Path(config_path).exists():
            with open(config_path, 'r') as f:
                user_config = yaml.safe_load(f)
                default_config.update(user_config)
        
        return default_config
    
    def search_single_dataset(self, dataset_id: str, title: str, agency: str, 
                            landing_url: str = "", last_seen: str = "") -> Dict:
        """Search for a single dataset across all recovery sources"""
        print(f"🔍 Searching for dataset: {title}")
        print(f"   Agency: {agency}")
        print(f"   ID: {dataset_id}")
        
        dataset_metadata = DatasetMetadata(
            title=title,
            agency=agency,
            data_gov_id=dataset_id,
            landing_url=landing_url,
            last_seen=last_seen,
            keywords=[]
        )
        
        results = self.recovery_system.search_dataset(dataset_metadata)
        provenance_pack = self.recovery_system.generate_provenance_pack(dataset_metadata, results)
        
        # Display results
        self.display_results(results, provenance_pack)
        
        return provenance_pack
    
    def search_from_file(self, input_file: str) -> List[Dict]:
        """Search for multiple datasets from a CSV/JSON file"""
        input_path = Path(input_file)
        if not input_path.exists():
            print(f"❌ Input file not found: {input_file}")
            return []
        
        datasets = self.load_datasets_from_file(input_path)
        results = []
        
        print(f"📋 Processing {len(datasets)} datasets from {input_file}")
        
        for i, dataset in enumerate(datasets, 1):
            print(f"\n[{i}/{len(datasets)}] Processing: {dataset.get('title', 'Unknown')}")
            
            provenance_pack = self.search_single_dataset(
                dataset.get('dataset_id', ''),
                dataset.get('title', ''),
                dataset.get('agency', ''),
                dataset.get('landing_url', ''),
                dataset.get('last_seen', '')
            )
            
            results.append(provenance_pack)
            
            # Save individual result
            self.save_provenance_pack(provenance_pack)
        
        return results
    
    def load_datasets_from_file(self, file_path: Path) -> List[Dict]:
        """Load datasets from CSV or JSON file"""
        if file_path.suffix.lower() == '.json':
            with open(file_path, 'r') as f:
                return json.load(f)
        elif file_path.suffix.lower() == '.csv':
            import csv
            datasets = []
            with open(file_path, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    datasets.append(row)
            return datasets
        else:
            print(f"❌ Unsupported file format: {file_path.suffix}")
            return []
    
    def display_results(self, results: List, provenance_pack: Dict):
        """Display search results in a formatted way"""
        if not results:
            print("   🚨 No recovery sources found")
            print("   💡 Consider filing a FOIA request")
            return
        
        print(f"   ✅ Found {len(results)} recovery source(s)")
        
        # Sort by confidence
        sorted_results = sorted(results, key=lambda x: x.confidence, reverse=True)
        
        for result in sorted_results:
            status_icon = self.get_status_icon(result.status)
            confidence_pct = int(result.confidence * 100)
            
            print(f"   {status_icon} {result.source}")
            print(f"      URL: {result.url}")
            print(f"      Confidence: {confidence_pct}%")
            if result.capture_date:
                print(f"      Captured: {result.capture_date}")
            print()
        
        print(f"   📋 Provenance Chain: {' → '.join(provenance_pack['provenance_chain'])}")
    
    def get_status_icon(self, status: RecoveryStatus) -> str:
        """Get icon for recovery status"""
        icons = {
            RecoveryStatus.FOUND_LIL: "🏛️",
            RecoveryStatus.FOUND_WAYBACK: "🕰️",
            RecoveryStatus.FOUND_DATALUMOS: "📊",
            RecoveryStatus.FOUND_EDGI: "🌱",
            RecoveryStatus.FOUND_EOT: "📚",
            RecoveryStatus.FOUND_STATE: "🏛️",
            RecoveryStatus.REISSUED: "♻️",
            RecoveryStatus.MISSING: "🚨",
            RecoveryStatus.FOIA_FILED: "📋"
        }
        return icons.get(status, "❓")
    
    def save_provenance_pack(self, provenance_pack: Dict):
        """Save provenance pack to file"""
        output_dir = Path(self.config['output_dir'])
        output_dir.mkdir(exist_ok=True)
        
        dataset_id = provenance_pack['dataset']['data_gov_id']
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        filename = f"{dataset_id}_{timestamp}_provenance.json"
        filepath = output_dir / filename
        
        with open(filepath, 'w') as f:
            json.dump(provenance_pack, f, indent=2)
        
        print(f"   💾 Saved provenance pack: {filepath}")
    
    def generate_foia_template(self, dataset_metadata: DatasetMetadata) -> str:
        """Generate FOIA request template"""
        template = f"""
FOIA REQUEST TEMPLATE
====================

To: {dataset_metadata.agency} FOIA Officer

Subject: FOIA Request - Missing Dataset: {dataset_metadata.title}

Dear FOIA Officer,

I am writing to request access to the following dataset under the Freedom of Information Act (5 U.S.C. § 552):

Dataset Information:
- Title: {dataset_metadata.title}
- Agency: {dataset_metadata.agency}
- Data.gov ID: {dataset_metadata.data_gov_id}
- Last Known URL: {dataset_metadata.landing_url}
- Last Seen: {dataset_metadata.last_seen}

This dataset was previously available on Data.gov but appears to have been removed or relocated. 
I am requesting access to this dataset for research purposes related to government transparency 
and data accessibility.

Please provide:
1. The complete dataset in its original format
2. Any metadata or documentation associated with the dataset
3. Information about why the dataset was removed from public access
4. Any alternative locations where this data might be available

I am willing to pay reasonable fees for the processing of this request. Please contact me if 
you need any clarification or if there are any fees associated with this request.

Thank you for your time and consideration.

Sincerely,
[Your Name]
[Your Contact Information]
[Date]
"""
        return template

def main():
    parser = argparse.ArgumentParser(
        description="Dataset Recovery CLI - Search for vanished government datasets",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Search for a single dataset
  python rescue_datasets_cli.py search --id "844dbad1-ee1e-44b8-9799-34cb7ed24640" \\
    --title "Electric Vehicle Population Data" --agency "State of Washington"
  
  # Search from CSV file
  python rescue_datasets_cli.py batch --input vanished_datasets.csv
  
  # Generate FOIA template
  python rescue_datasets_cli.py foia --id "844dbad1-ee1e-44b8-9799-34cb7ed24640" \\
    --title "Electric Vehicle Population Data" --agency "State of Washington"
  
  # List available recovery sources
  python rescue_datasets_cli.py sources
        """
    )
    
    parser.add_argument('--config', '-c', help='Path to configuration YAML file')
    parser.add_argument('--output-dir', '-o', help='Output directory for results')
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Search command
    search_parser = subparsers.add_parser('search', help='Search for a single dataset')
    search_parser.add_argument('--id', required=True, help='Dataset ID')
    search_parser.add_argument('--title', required=True, help='Dataset title')
    search_parser.add_argument('--agency', required=True, help='Agency name')
    search_parser.add_argument('--url', help='Landing page URL')
    search_parser.add_argument('--last-seen', help='Last seen date')
    
    # Batch command
    batch_parser = subparsers.add_parser('batch', help='Search multiple datasets from file')
    batch_parser.add_argument('--input', '-i', required=True, help='Input CSV or JSON file')
    
    # FOIA command
    foia_parser = subparsers.add_parser('foia', help='Generate FOIA request template')
    foia_parser.add_argument('--id', required=True, help='Dataset ID')
    foia_parser.add_argument('--title', required=True, help='Dataset title')
    foia_parser.add_argument('--agency', required=True, help='Agency name')
    foia_parser.add_argument('--url', help='Landing page URL')
    foia_parser.add_argument('--last-seen', help='Last seen date')
    
    # Sources command
    subparsers.add_parser('sources', help='List available recovery sources')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Initialize CLI
    cli = RescueDatasetsCLI(args.config)
    
    if args.output_dir:
        cli.config['output_dir'] = args.output_dir
    
    try:
        if args.command == 'search':
            cli.search_single_dataset(
                args.id, args.title, args.agency, 
                args.url or "", args.last_seen or ""
            )
        
        elif args.command == 'batch':
            cli.search_from_file(args.input)
        
        elif args.command == 'foia':
            dataset_metadata = DatasetMetadata(
                title=args.title,
                agency=args.agency,
                data_gov_id=args.id,
                landing_url=args.url or "",
                last_seen=args.last_seen or "",
                keywords=[]
            )
            template = cli.generate_foia_template(dataset_metadata)
            print(template)
        
        elif args.command == 'sources':
            print("Available Recovery Sources:")
            print("=" * 50)
            for source_id, source_info in cli.recovery_system.recovery_sources.items():
                print(f"{source_info['name']}")
                print(f"  Description: {source_info['description']}")
                print(f"  Priority: {source_info['priority']}")
                print(f"  API: {source_info['api_url']}")
                print()
    
    except KeyboardInterrupt:
        print("\n⏹️  Operation cancelled by user")
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
