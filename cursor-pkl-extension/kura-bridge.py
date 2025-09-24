#!/usr/bin/env python3
"""
Kura Integration Bridge for PKL Extension

This service converts PKL sessions to Kura conversation format and provides
advanced conversation analysis capabilities including:
- Hierarchical clustering
- Automatic intent discovery  
- UMAP visualization
- Pattern mining

Usage:
    python kura-bridge.py --sessions-file path/to/sessions.json --output-dir ./kura_analysis
"""

import asyncio
import json
import os
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
import argparse

# Kura imports
from kura.types import Conversation
from kura.cache import DiskCacheStrategy
from kura.summarisation import summarise_conversations, SummaryModel
from kura.cluster import generate_base_clusters_from_conversation_summaries, ClusterDescriptionModel
from kura.meta_cluster import reduce_clusters_from_base_clusters, MetaClusterModel
from kura.dimensionality import reduce_dimensionality_from_clusters, HDBUMAP
from kura.visualization import visualise_pipeline_results
from kura.checkpoints import JSONLCheckpointManager
from rich.console import Console


class PKLKuraBridge:
    """Bridge service to convert PKL sessions to Kura conversations and run analysis"""
    
    def __init__(self, cache_dir: str = "./.kura_cache", output_dir: str = "./kura_analysis"):
        self.cache_dir = Path(cache_dir)
        self.output_dir = Path(output_dir)
        self.console = Console()
        
        # Ensure directories exist
        self.cache_dir.mkdir(exist_ok=True)
        self.output_dir.mkdir(exist_ok=True)
        
        # Initialize Kura models
        self.summary_model = SummaryModel(
            console=self.console,
            cache=DiskCacheStrategy(cache_dir=str(self.cache_dir / "summary"))
        )
        self.cluster_model = ClusterDescriptionModel(console=self.console)
        self.meta_cluster_model = MetaClusterModel(console=self.console)
        self.dimensionality_model = HDBUMAP()
        
        # Checkpoint manager
        self.checkpoint_manager = JSONLCheckpointManager(
            str(self.output_dir / "checkpoints"), 
            enabled=True
        )

    def convert_pkl_sessions_to_conversations(self, sessions_data: List[Dict]) -> List[Conversation]:
        """Convert PKL session format to Kura conversation format"""
        conversations = []
        
        for session in sessions_data:
            try:
                # Extract conversation events if they exist
                conversation_events = []
                
                # Check if session has conversation events
                if 'conversationEvents' in session:
                    conversation_events = session['conversationEvents']
                elif 'linkedEvents' in session:
                    # Convert linked events to conversation format
                    for event in session['linkedEvents']:
                        if event.get('type') in ['code_run', 'success', 'error']:
                            conversation_events.append({
                                'role': 'user' if event.get('type') == 'code_run' else 'assistant',
                                'content': event.get('output', ''),
                                'timestamp': event.get('timestamp')
                            })
                
                # If no conversation events, create synthetic conversation from session data
                if not conversation_events:
                    conversation_events = self._create_synthetic_conversation(session)
                
                # Convert to Kura conversation format
                messages = []
                for event in conversation_events:
                    messages.append({
                        "role": event.get('role', 'user'),
                        "content": event.get('content', ''),
                        "timestamp": event.get('timestamp', session.get('timestamp'))
                    })
                
                # Create Kura Conversation object
                conversation = Conversation(
                    id=session.get('id', f"session_{len(conversations)}"),
                    messages=messages,
                    metadata={
                        'intent': session.get('intent'),
                        'phase': session.get('phase'),
                        'outcome': session.get('outcome'),
                        'confidence': session.get('confidence'),
                        'currentFile': session.get('currentFile'),
                        'timestamp': session.get('timestamp'),
                        'privacyMode': session.get('privacyMode', False)
                    }
                )
                
                conversations.append(conversation)
                
            except Exception as e:
                self.console.print(f"[red]Error converting session {session.get('id', 'unknown')}: {e}[/red]")
                continue
        
        self.console.print(f"[green]Converted {len(conversations)} PKL sessions to Kura conversations[/green]")
        return conversations

    def _create_synthetic_conversation(self, session: Dict) -> List[Dict]:
        """Create a synthetic conversation from session data when no conversation events exist"""
        conversation = []
        
        # Create user message based on intent and context
        intent = session.get('intent', 'unknown')
        current_file = session.get('currentFile', 'unknown file')
        
        user_content = f"Working on {intent} task in {current_file}"
        
        # Add code changes as context
        if session.get('codeDeltas'):
            code_changes = []
            for delta in session['codeDeltas'][:3]:  # Limit to first 3 changes
                if delta.get('afterContent'):
                    code_changes.append(f"Added/modified: {delta['afterContent'][:200]}...")
            
            if code_changes:
                user_content += f"\n\nCode changes:\n" + "\n".join(code_changes)
        
        conversation.append({
            'role': 'user',
            'content': user_content,
            'timestamp': session.get('timestamp')
        })
        
        # Create assistant response based on outcome
        outcome = session.get('outcome', 'in-progress')
        phase = session.get('phase', 'start')
        
        if outcome == 'success':
            assistant_content = f"Successfully completed the {intent} task. The solution worked as expected."
        elif outcome == 'stuck':
            assistant_content = f"Encountered difficulties with the {intent} task. May need alternative approach."
        else:
            assistant_content = f"Working on the {intent} task, currently in {phase} phase."
        
        # Add file changes as context
        if session.get('fileChanges'):
            file_changes = [f"Modified {fc.get('filePath', 'file')}" for fc in session['fileChanges'][:3]]
            if file_changes:
                assistant_content += f"\n\nFiles affected: {', '.join(file_changes)}"
        
        conversation.append({
            'role': 'assistant', 
            'content': assistant_content,
            'timestamp': session.get('endTime', session.get('timestamp'))
        })
        
        return conversation

    async def analyze_conversations(self, conversations: List[Conversation]) -> Dict[str, Any]:
        """Run Kura analysis pipeline on conversations"""
        
        if not conversations:
            raise ValueError("No conversations to analyze")
        
        self.console.print(f"[blue]Starting Kura analysis pipeline with {len(conversations)} conversations[/blue]")
        
        # Step 1: Summarize conversations
        self.console.print("[blue]Step 1: Summarizing conversations...[/blue]")
        summaries = await summarise_conversations(
            conversations, 
            model=self.summary_model, 
            checkpoint_manager=self.checkpoint_manager
        )
        
        # Step 2: Generate base clusters
        self.console.print("[blue]Step 2: Generating base clusters...[/blue]")
        clusters = await generate_base_clusters_from_conversation_summaries(
            summaries, 
            model=self.cluster_model, 
            checkpoint_manager=self.checkpoint_manager
        )
        
        # Step 3: Reduce clusters into hierarchy
        self.console.print("[blue]Step 3: Creating hierarchical clusters...[/blue]")
        reduced_clusters = await reduce_clusters_from_base_clusters(
            clusters, 
            model=self.meta_cluster_model, 
            checkpoint_manager=self.checkpoint_manager
        )
        
        # Step 4: Generate UMAP projections
        self.console.print("[blue]Step 4: Generating UMAP projections...[/blue]")
        projected_clusters = await reduce_dimensionality_from_clusters(
            reduced_clusters,
            model=self.dimensionality_model,
            checkpoint_manager=self.checkpoint_manager
        )
        
        # Step 5: Visualize results
        self.console.print("[blue]Step 5: Visualizing results...[/blue]")
        visualise_pipeline_results(projected_clusters, style="rich")
        
        # Save results
        results = {
            'summaries': [s.dict() for s in summaries] if hasattr(summaries[0], 'dict') else summaries,
            'clusters': [c.dict() for c in clusters] if hasattr(clusters[0], 'dict') else clusters,
            'reduced_clusters': [rc.dict() for rc in reduced_clusters] if hasattr(reduced_clusters[0], 'dict') else reduced_clusters,
            'projected_clusters': [pc.dict() for pc in projected_clusters] if hasattr(projected_clusters[0], 'dict') else projected_clusters,
            'analysis_timestamp': datetime.now().isoformat(),
            'total_conversations': len(conversations)
        }
        
        # Save to file
        results_file = self.output_dir / "kura_analysis_results.json"
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        self.console.print(f"[green]Analysis complete! Results saved to {results_file}[/green]")
        
        return results

    def generate_dashboard_data(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate data structure for enhanced dashboard visualization"""
        
        dashboard_data = {
            'hierarchical_clusters': self._extract_hierarchical_structure(results.get('reduced_clusters', [])),
            'umap_coordinates': self._extract_umap_coordinates(results.get('projected_clusters', [])),
            'intent_patterns': self._extract_intent_patterns(results.get('summaries', [])),
            'success_patterns': self._extract_success_patterns(results.get('clusters', [])),
            'file_patterns': self._extract_file_patterns(results.get('summaries', [])),
            'temporal_patterns': self._extract_temporal_patterns(results.get('summaries', [])),
            'visualization_config': {
                'umap_plot': {
                    'width': 800,
                    'height': 600,
                    'point_size': 5,
                    'color_scheme': 'viridis'
                },
                'cluster_tree': {
                    'max_depth': 5,
                    'min_cluster_size': 3,
                    'show_confidence': True
                }
            }
        }
        
        # Save dashboard data
        dashboard_file = self.output_dir / "dashboard_data.json"
        with open(dashboard_file, 'w') as f:
            json.dump(dashboard_data, f, indent=2, default=str)
        
        self.console.print(f"[green]Dashboard data saved to {dashboard_file}[/green]")
        
        return dashboard_data

    def _extract_hierarchical_structure(self, reduced_clusters: List) -> Dict[str, Any]:
        """Extract hierarchical cluster structure for tree visualization"""
        # This would be implemented based on Kura's actual cluster structure
        # For now, return a placeholder structure
        return {
            'root': {
                'name': 'All Sessions',
                'size': len(reduced_clusters),
                'children': []
            }
        }

    def _extract_umap_coordinates(self, projected_clusters: List) -> List[Dict]:
        """Extract UMAP coordinates for scatter plot visualization"""
        coordinates = []
        for i, cluster in enumerate(projected_clusters):
            # Extract actual coordinates from Kura's projected clusters
            coordinates.append({
                'id': i,
                'x': 0.0,  # Would be actual UMAP x coordinate
                'y': 0.0,  # Would be actual UMAP y coordinate
                'cluster_id': getattr(cluster, 'id', i),
                'label': getattr(cluster, 'name', f'Cluster {i}')
            })
        return coordinates

    def _extract_intent_patterns(self, summaries: List) -> Dict[str, int]:
        """Extract intent patterns from conversation summaries"""
        intent_counts = {}
        for summary in summaries:
            if hasattr(summary, 'metadata') and summary.metadata:
                intent = summary.metadata.get('intent', 'unknown')
                intent_counts[intent] = intent_counts.get(intent, 0) + 1
        return intent_counts

    def _extract_success_patterns(self, clusters: List) -> Dict[str, Any]:
        """Extract success rate patterns by cluster"""
        success_patterns = {}
        for cluster in clusters:
            cluster_name = getattr(cluster, 'name', 'Unknown')
            # Would calculate actual success rates from cluster data
            success_patterns[cluster_name] = {
                'success_rate': 0.85,  # Placeholder
                'total_sessions': 10,   # Placeholder
                'avg_duration': 15      # Placeholder
            }
        return success_patterns

    def _extract_file_patterns(self, summaries: List) -> Dict[str, int]:
        """Extract file type patterns"""
        file_patterns = {}
        for summary in summaries:
            if hasattr(summary, 'metadata') and summary.metadata:
                current_file = summary.metadata.get('currentFile', '')
                if current_file:
                    ext = Path(current_file).suffix or 'no_extension'
                    file_patterns[ext] = file_patterns.get(ext, 0) + 1
        return file_patterns

    def _extract_temporal_patterns(self, summaries: List) -> List[Dict]:
        """Extract temporal patterns for timeline visualization"""
        temporal_data = []
        for summary in summaries:
            if hasattr(summary, 'metadata') and summary.metadata:
                timestamp = summary.metadata.get('timestamp')
                if timestamp:
                    temporal_data.append({
                        'timestamp': timestamp,
                        'intent': summary.metadata.get('intent'),
                        'outcome': summary.metadata.get('outcome')
                    })
        return sorted(temporal_data, key=lambda x: x['timestamp'])


async def main():
    """Main function to run PKL-Kura integration"""
    parser = argparse.ArgumentParser(description='PKL-Kura Integration Bridge')
    parser.add_argument('--sessions-file', required=True, help='Path to PKL sessions JSON file')
    parser.add_argument('--output-dir', default='./kura_analysis', help='Output directory for analysis results')
    parser.add_argument('--cache-dir', default='./.kura_cache', help='Cache directory for Kura models')
    
    args = parser.parse_args()
    
    # Initialize bridge
    bridge = PKLKuraBridge(cache_dir=args.cache_dir, output_dir=args.output_dir)
    
    # Load PKL sessions
    console = Console()
    console.print(f"[blue]Loading PKL sessions from {args.sessions_file}[/blue]")
    
    try:
        with open(args.sessions_file, 'r') as f:
            sessions_data = json.load(f)
        
        if not isinstance(sessions_data, list):
            sessions_data = [sessions_data]  # Handle single session
        
        console.print(f"[green]Loaded {len(sessions_data)} PKL sessions[/green]")
        
    except Exception as e:
        console.print(f"[red]Error loading sessions file: {e}[/red]")
        return
    
    # Convert to Kura format
    conversations = bridge.convert_pkl_sessions_to_conversations(sessions_data)
    
    if not conversations:
        console.print("[red]No valid conversations found after conversion[/red]")
        return
    
    # Run Kura analysis
    try:
        results = await bridge.analyze_conversations(conversations)
        
        # Generate dashboard data
        dashboard_data = bridge.generate_dashboard_data(results)
        
        console.print("[green]PKL-Kura integration completed successfully![/green]")
        console.print(f"[blue]Results available in: {args.output_dir}[/blue]")
        
    except Exception as e:
        console.print(f"[red]Error during analysis: {e}[/red]")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
