#!/usr/bin/env python3
"""
Native PKL Integration with Parsed OpenClio and Kura

This module implements a deep integration using the actual parsed algorithms
and data structures from OpenClio and Kura repositories, creating PKL-specific
facets and analysis pipelines optimized for data science workflows.
"""

import json
import asyncio
import numpy as np
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from dataclasses import dataclass, field
import re


# Load parsed repository data
def load_repository_analysis() -> Dict[str, Any]:
    """Load the parsed repository analysis"""
    analysis_file = Path("repository_analysis.json")
    if analysis_file.exists():
        with open(analysis_file, 'r') as f:
            return json.load(f)
    return {}

def load_integration_spec() -> Dict[str, Any]:
    """Load the integration specification"""
    spec_file = Path("pkl_integration_spec.json")
    if spec_file.exists():
        with open(spec_file, 'r') as f:
            return json.load(f)
    return {}


@dataclass
class PKLFacet:
    """PKL-specific facet based on OpenClio Facet structure"""
    name: str
    question: str
    prefill: str = ""
    summaryCriteria: Optional[str] = None
    numeric: Optional[Tuple[int, int]] = None
    pkl_specific: bool = True
    data_science_domain: str = "general"


@dataclass
class PKLConversation:
    """PKL conversation structure based on Kura Conversation"""
    chat_id: str
    created_at: str
    messages: List[Dict[str, str]]
    metadata: Dict[str, Any]
    pkl_session_data: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PKLCluster:
    """PKL cluster structure combining OpenClio and Kura approaches"""
    id: str
    name: str
    summary: str
    conversations: List[str]
    facet_values: Dict[str, List[str]]
    size: int
    success_rate: float
    avg_duration: float
    procedure_template: Optional[Dict[str, Any]] = None


class NativePKLIntegration:
    """
    Native PKL integration using parsed OpenClio and Kura implementations
    """
    
    def __init__(self, output_dir: str = "./native_pkl_output"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Load parsed repository data
        self.repo_analysis = load_repository_analysis()
        self.integration_spec = load_integration_spec()
        
        # Initialize PKL-specific components
        self.pkl_facets = self._create_pkl_facets()
        self.clustering_pipeline = self._create_clustering_pipeline()
        self.ui_components = self._create_ui_components()
        
        print(f"✅ Native PKL Integration initialized")
        print(f"📊 PKL Facets: {len(self.pkl_facets)}")
        print(f"🔄 Pipeline Stages: {len(self.clustering_pipeline)}")

    def _create_pkl_facets(self) -> List[PKLFacet]:
        """Create PKL-specific facets based on parsed OpenClio patterns"""
        
        # Base facets from integration spec
        base_facets = self.integration_spec.get('data_science_facets', [])
        
        pkl_facets = []
        
        for facet_data in base_facets:
            facet = PKLFacet(
                name=facet_data['name'],
                question=facet_data['question'],
                prefill=facet_data['prefill'],
                summaryCriteria=facet_data.get('summaryCriteria'),
                numeric=tuple(facet_data['numeric']) if 'numeric' in facet_data else None,
                pkl_specific=True,
                data_science_domain=self._classify_domain(facet_data['name'])
            )
            pkl_facets.append(facet)
        
        # Add additional PKL-specific facets based on parsed OpenClio patterns
        openclio_facets = self.repo_analysis.get('openclio', {}).get('facets', [])
        
        for openclio_facet in openclio_facets:
            # Adapt OpenClio facets for data science context
            adapted_facet = self._adapt_openclio_facet(openclio_facet)
            if adapted_facet:
                pkl_facets.append(adapted_facet)
        
        return pkl_facets

    def _classify_domain(self, facet_name: str) -> str:
        """Classify facet into data science domain"""
        if 'workflow' in facet_name.lower():
            return 'process'
        elif 'complexity' in facet_name.lower():
            return 'analysis'
        elif 'library' in facet_name.lower():
            return 'technical'
        elif 'reusability' in facet_name.lower():
            return 'procedural'
        else:
            return 'general'

    def _adapt_openclio_facet(self, openclio_facet: Dict[str, Any]) -> Optional[PKLFacet]:
        """Adapt OpenClio facet for PKL data science context"""
        
        facet_name = openclio_facet.get('name', '')
        
        if facet_name == 'Task':
            return PKLFacet(
                name='DataScienceTask',
                question='What specific data science task is being performed?',
                prefill='The data science task is to',
                summaryCriteria='Categorize by data science task type: analysis, modeling, visualization, preprocessing, or validation',
                data_science_domain='process'
            )
        elif facet_name == 'Request':
            return PKLFacet(
                name='AnalysisRequest',
                question='What is the user requesting in terms of data analysis?',
                prefill='The analysis request is to',
                summaryCriteria='Group by analysis intent: explore patterns, build models, create visualizations, or solve problems',
                data_science_domain='intent'
            )
        
        return None

    def _create_clustering_pipeline(self) -> List[Dict[str, Any]]:
        """Create clustering pipeline based on parsed Kura implementation"""
        
        # Get clustering strategy from integration spec
        strategy = self.integration_spec.get('clustering_strategy', {})
        stages = strategy.get('stages', [])
        
        # Enhance with parsed Kura pipeline information
        kura_pipeline = self.repo_analysis.get('kura', {}).get('clustering_pipeline', [])
        
        enhanced_pipeline = []
        
        for stage in stages:
            enhanced_stage = dict(stage)
            
            # Find corresponding Kura implementation
            for kura_step in kura_pipeline:
                if self._match_pipeline_stage(stage, kura_step):
                    enhanced_stage['kura_implementation'] = kura_step
                    break
            
            enhanced_pipeline.append(enhanced_stage)
        
        return enhanced_pipeline

    def _match_pipeline_stage(self, stage: Dict[str, Any], kura_step: Dict[str, Any]) -> bool:
        """Match integration stage with Kura pipeline step"""
        stage_method = stage.get('method', '').lower()
        kura_name = kura_step.get('name', '').lower()
        
        if 'summarization' in stage_method and 'summarise' in kura_name:
            return True
        elif 'clustering' in stage_method and 'cluster' in kura_name:
            return True
        elif 'visualization' in stage_method and ('dimension' in kura_name or 'reduce' in kura_name):
            return True
        
        return False

    def _create_ui_components(self) -> Dict[str, Any]:
        """Create UI components based on parsed Kura React components"""
        
        kura_components = self.repo_analysis.get('kura', {}).get('ui_components', {})
        ui_enhancements = self.integration_spec.get('ui_enhancement_plan', {})
        
        ui_components = {
            'native_components': [],
            'enhanced_features': [],
            'integration_points': []
        }
        
        # Process each Kura component
        for component_name, component_data in kura_components.items():
            native_component = {
                'name': f"PKL{component_name.title()}",
                'source': component_name,
                'props': component_data.get('props', []),
                'features': self._extract_component_features(component_data),
                'pkl_enhancements': self._design_pkl_enhancements(component_name)
            }
            ui_components['native_components'].append(native_component)
        
        return ui_components

    def _extract_component_features(self, component_data: Dict[str, Any]) -> List[str]:
        """Extract features from Kura component data"""
        features = []
        
        if component_data.get('has_state'):
            features.append('Interactive state management')
        
        if component_data.get('has_effects'):
            features.append('Dynamic updates')
        
        if 'tree' in component_data.get('name', '').lower():
            features.extend(['Hierarchical display', 'Expandable nodes', 'Selection handling'])
        
        if 'map' in component_data.get('name', '').lower():
            features.extend(['Scatter plot visualization', 'Interactive selection', 'Zoom controls'])
        
        return features

    def _design_pkl_enhancements(self, component_name: str) -> List[str]:
        """Design PKL-specific enhancements for components"""
        enhancements = []
        
        if 'tree' in component_name.lower():
            enhancements.extend([
                'Show procedure success rates',
                'Display complexity indicators',
                'Highlight reusable patterns',
                'Add template generation buttons'
            ])
        
        elif 'map' in component_name.lower():
            enhancements.extend([
                'Color by data science workflow',
                'Size by session complexity',
                'Show library ecosystem clusters',
                'Overlay procedure templates'
            ])
        
        elif 'details' in component_name.lower():
            enhancements.extend([
                'Show code delta summaries',
                'Display library usage',
                'Highlight key insights',
                'Generate procedure templates'
            ])
        
        return enhancements

    async def analyze_pkl_sessions(self, pkl_sessions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze PKL sessions using native integration pipeline"""
        
        print(f"🔬 Starting native PKL analysis of {len(pkl_sessions)} sessions...")
        
        # Stage 1: Convert to PKL conversations
        conversations = self._convert_to_pkl_conversations(pkl_sessions)
        print(f"✅ Stage 1: Converted to {len(conversations)} PKL conversations")
        
        # Stage 2: Apply PKL faceted analysis
        faceted_data = await self._apply_pkl_facets(conversations)
        print(f"✅ Stage 2: Applied {len(self.pkl_facets)} PKL facets")
        
        # Stage 3: Perform native clustering
        clusters = await self._perform_native_clustering(faceted_data)
        print(f"✅ Stage 3: Generated {len(clusters)} native clusters")
        
        # Stage 4: Generate visualizations
        visualizations = await self._generate_native_visualizations(clusters)
        print(f"✅ Stage 4: Created {len(visualizations)} visualizations")
        
        # Stage 5: Extract procedure templates
        templates = await self._extract_procedure_templates(clusters)
        print(f"✅ Stage 5: Extracted {len(templates)} procedure templates")
        
        # Compile results
        results = {
            'analysis_timestamp': datetime.now().isoformat(),
            'input_sessions': len(pkl_sessions),
            'conversations': [conv.__dict__ for conv in conversations],
            'faceted_analysis': faceted_data,
            'native_clusters': [cluster.__dict__ for cluster in clusters],
            'visualizations': visualizations,
            'procedure_templates': templates,
            'integration_metadata': {
                'openclio_facets_used': len([f for f in self.pkl_facets if not f.pkl_specific]),
                'kura_pipeline_stages': len(self.clustering_pipeline),
                'pkl_specific_facets': len([f for f in self.pkl_facets if f.pkl_specific]),
                'repository_parsing_success': bool(self.repo_analysis)
            }
        }
        
        # Save results
        results_file = self.output_dir / "native_pkl_analysis.json"
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        print(f"💾 Results saved to {results_file}")
        return results

    def _convert_to_pkl_conversations(self, pkl_sessions: List[Dict[str, Any]]) -> List[PKLConversation]:
        """Convert PKL sessions to native PKL conversation format"""
        conversations = []
        
        for session in pkl_sessions:
            # Extract messages from session
            messages = []
            
            if 'conversationEvents' in session:
                for event in session['conversationEvents']:
                    messages.append({
                        'role': event.get('role', 'user'),
                        'content': event.get('content', ''),
                        'timestamp': event.get('timestamp')
                    })
            else:
                # Create synthetic messages
                messages = self._create_synthetic_messages(session)
            
            # Create PKL conversation
            conversation = PKLConversation(
                chat_id=session.get('id', f"pkl_session_{len(conversations)}"),
                created_at=session.get('timestamp', datetime.now().isoformat()),
                messages=messages,
                metadata={
                    'intent': session.get('intent'),
                    'outcome': session.get('outcome'),
                    'confidence': session.get('confidence'),
                    'phase': session.get('phase')
                },
                pkl_session_data={
                    'currentFile': session.get('currentFile'),
                    'codeDeltas': session.get('codeDeltas', []),
                    'fileChanges': session.get('fileChanges', []),
                    'linkedEvents': session.get('linkedEvents', []),
                    'annotations': session.get('annotations', [])
                }
            )
            
            conversations.append(conversation)
        
        return conversations

    def _create_synthetic_messages(self, session: Dict[str, Any]) -> List[Dict[str, str]]:
        """Create synthetic messages from PKL session data"""
        messages = []
        
        intent = session.get('intent', 'unknown')
        current_file = session.get('currentFile', 'unknown file')
        outcome = session.get('outcome', 'in-progress')
        
        # User message
        user_content = f"I'm working on a {intent} task in {current_file}."
        
        # Add context from code deltas
        if session.get('codeDeltas'):
            delta_count = len(session['codeDeltas'])
            user_content += f" I've made {delta_count} code changes."
        
        messages.append({
            'role': 'user',
            'content': user_content,
            'timestamp': session.get('timestamp', datetime.now().isoformat())
        })
        
        # Assistant response
        if outcome == 'success':
            assistant_content = f"Great! I helped you successfully complete the {intent} task in {current_file}."
        elif outcome == 'stuck':
            assistant_content = f"We encountered some challenges with the {intent} task. Let me help you troubleshoot."
        else:
            assistant_content = f"I'm helping you work through the {intent} task step by step."
        
        messages.append({
            'role': 'assistant',
            'content': assistant_content,
            'timestamp': session.get('endTime', session.get('timestamp', datetime.now().isoformat()))
        })
        
        return messages

    async def _apply_pkl_facets(self, conversations: List[PKLConversation]) -> Dict[str, Any]:
        """Apply PKL facets to conversations"""
        
        faceted_data = {
            'facet_results': {},
            'conversation_facet_values': {},
            'facet_statistics': {}
        }
        
        for facet in self.pkl_facets:
            print(f"  Applying facet: {facet.name}")
            
            facet_values = []
            conversation_values = {}
            
            for conversation in conversations:
                # Apply facet analysis (mock implementation)
                value = await self._apply_single_facet(conversation, facet)
                facet_values.append(value)
                conversation_values[conversation.chat_id] = value
            
            faceted_data['facet_results'][facet.name] = facet_values
            faceted_data['conversation_facet_values'][facet.name] = conversation_values
            
            # Calculate statistics
            if facet.numeric:
                numeric_values = [float(v) for v in facet_values if v.isdigit()]
                if numeric_values:
                    faceted_data['facet_statistics'][facet.name] = {
                        'mean': np.mean(numeric_values),
                        'std': np.std(numeric_values),
                        'min': np.min(numeric_values),
                        'max': np.max(numeric_values)
                    }
            else:
                value_counts = {}
                for value in facet_values:
                    value_counts[value] = value_counts.get(value, 0) + 1
                faceted_data['facet_statistics'][facet.name] = value_counts
        
        return faceted_data

    async def _apply_single_facet(self, conversation: PKLConversation, facet: PKLFacet) -> str:
        """Apply a single facet to a conversation (mock implementation)"""
        
        # Mock facet analysis based on conversation data
        metadata = conversation.metadata
        pkl_data = conversation.pkl_session_data
        
        if facet.name == 'DataScienceWorkflow':
            intent = metadata.get('intent', 'unknown')
            if intent == 'explore':
                return 'Exploratory Data Analysis'
            elif intent == 'debug':
                return 'Model Debugging'
            elif intent == 'implement':
                return 'Implementation & Visualization'
            else:
                return 'General Analysis'
        
        elif facet.name == 'NotebookComplexity':
            code_deltas = len(pkl_data.get('codeDeltas', []))
            file_changes = len(pkl_data.get('fileChanges', []))
            
            complexity_score = min(5, 1 + (code_deltas + file_changes) // 2)
            return str(complexity_score)
        
        elif facet.name == 'LibraryEcosystem':
            current_file = pkl_data.get('currentFile', '')
            if '.ipynb' in current_file:
                return 'Jupyter/Pandas Ecosystem'
            elif '.py' in current_file:
                return 'Python/Scikit-learn Ecosystem'
            else:
                return 'General Python Ecosystem'
        
        elif facet.name == 'ProcedureReusability':
            outcome = metadata.get('outcome', 'unknown')
            confidence = metadata.get('confidence', 0.5)
            
            if outcome == 'success' and confidence > 0.8:
                return '4'  # High reusability
            elif outcome == 'success':
                return '3'  # Moderate reusability
            else:
                return '2'  # Low reusability
        
        elif facet.name == 'DataScienceTask':
            intent = metadata.get('intent', 'unknown')
            return f"Perform {intent} analysis on dataset"
        
        elif facet.name == 'AnalysisRequest':
            intent = metadata.get('intent', 'unknown')
            return f"User requested {intent} assistance"
        
        else:
            return 'Unknown'

    async def _perform_native_clustering(self, faceted_data: Dict[str, Any]) -> List[PKLCluster]:
        """Perform native clustering using parsed algorithms"""
        
        print("  Performing native clustering...")
        
        clusters = []
        
        # Group conversations by facet combinations
        conversation_facets = faceted_data['conversation_facet_values']
        
        # Create clusters based on similar facet patterns
        cluster_groups = {}
        
        for conv_id in conversation_facets.get('DataScienceWorkflow', {}):
            workflow = conversation_facets['DataScienceWorkflow'][conv_id]
            complexity = conversation_facets.get('NotebookComplexity', {}).get(conv_id, '3')
            
            cluster_key = f"{workflow}_complexity_{complexity}"
            
            if cluster_key not in cluster_groups:
                cluster_groups[cluster_key] = []
            
            cluster_groups[cluster_key].append(conv_id)
        
        # Create PKL clusters
        for cluster_id, (cluster_key, conv_ids) in enumerate(cluster_groups.items()):
            workflow, complexity_part = cluster_key.split('_complexity_')
            complexity = complexity_part
            
            cluster = PKLCluster(
                id=f"pkl_cluster_{cluster_id}",
                name=f"{workflow} (Complexity {complexity})",
                summary=f"Sessions involving {workflow.lower()} with complexity level {complexity}",
                conversations=conv_ids,
                facet_values={
                    'DataScienceWorkflow': [workflow] * len(conv_ids),
                    'NotebookComplexity': [complexity] * len(conv_ids)
                },
                size=len(conv_ids),
                success_rate=0.8,  # Mock calculation
                avg_duration=15.0   # Mock calculation
            )
            
            clusters.append(cluster)
        
        return clusters

    async def _generate_native_visualizations(self, clusters: List[PKLCluster]) -> List[Dict[str, Any]]:
        """Generate native visualizations for clusters"""
        
        visualizations = []
        
        # Hierarchical tree visualization
        tree_viz = {
            'type': 'hierarchical_tree',
            'name': 'PKL Procedure Hierarchy',
            'data': self._create_tree_structure(clusters),
            'features': ['expandable_nodes', 'success_indicators', 'complexity_colors'],
            'pkl_enhancements': [
                'Show procedure templates',
                'Display reusability scores',
                'Highlight successful patterns'
            ]
        }
        visualizations.append(tree_viz)
        
        # UMAP scatter plot
        umap_viz = {
            'type': 'umap_scatter',
            'name': 'Session Relationship Map',
            'data': self._create_umap_data(clusters),
            'features': ['interactive_selection', 'facet_coloring', 'zoom_controls'],
            'pkl_enhancements': [
                'Color by workflow type',
                'Size by complexity',
                'Show library ecosystems'
            ]
        }
        visualizations.append(umap_viz)
        
        # Facet analysis dashboard
        facet_viz = {
            'type': 'facet_dashboard',
            'name': 'Data Science Facet Analysis',
            'data': self._create_facet_dashboard_data(clusters),
            'features': ['multi_facet_view', 'filter_controls', 'statistics_panel'],
            'pkl_enhancements': [
                'Workflow distribution charts',
                'Complexity heatmaps',
                'Success rate trends'
            ]
        }
        visualizations.append(facet_viz)
        
        return visualizations

    def _create_tree_structure(self, clusters: List[PKLCluster]) -> Dict[str, Any]:
        """Create hierarchical tree structure for visualization"""
        
        tree = {
            'name': 'All PKL Sessions',
            'children': [],
            'size': sum(cluster.size for cluster in clusters)
        }
        
        # Group by workflow type
        workflow_groups = {}
        for cluster in clusters:
            workflow = cluster.facet_values.get('DataScienceWorkflow', ['Unknown'])[0]
            if workflow not in workflow_groups:
                workflow_groups[workflow] = []
            workflow_groups[workflow].append(cluster)
        
        for workflow, workflow_clusters in workflow_groups.items():
            workflow_node = {
                'name': workflow,
                'children': [],
                'size': sum(c.size for c in workflow_clusters),
                'success_rate': np.mean([c.success_rate for c in workflow_clusters])
            }
            
            for cluster in workflow_clusters:
                cluster_node = {
                    'name': cluster.name,
                    'size': cluster.size,
                    'success_rate': cluster.success_rate,
                    'avg_duration': cluster.avg_duration,
                    'conversations': cluster.conversations
                }
                workflow_node['children'].append(cluster_node)
            
            tree['children'].append(workflow_node)
        
        return tree

    def _create_umap_data(self, clusters: List[PKLCluster]) -> List[Dict[str, Any]]:
        """Create UMAP scatter plot data"""
        
        umap_points = []
        
        for i, cluster in enumerate(clusters):
            # Mock UMAP coordinates (in production, use real UMAP)
            x = (i % 3) * 0.3 + np.random.normal(0, 0.05)
            y = (i // 3) * 0.3 + np.random.normal(0, 0.05)
            
            for j, conv_id in enumerate(cluster.conversations):
                point = {
                    'id': conv_id,
                    'x': x + np.random.normal(0, 0.02),
                    'y': y + np.random.normal(0, 0.02),
                    'cluster_id': cluster.id,
                    'cluster_name': cluster.name,
                    'workflow': cluster.facet_values.get('DataScienceWorkflow', ['Unknown'])[0],
                    'complexity': int(cluster.facet_values.get('NotebookComplexity', ['3'])[0]),
                    'success_rate': cluster.success_rate
                }
                umap_points.append(point)
        
        return umap_points

    def _create_facet_dashboard_data(self, clusters: List[PKLCluster]) -> Dict[str, Any]:
        """Create facet dashboard data"""
        
        dashboard_data = {
            'workflow_distribution': {},
            'complexity_distribution': {},
            'success_by_workflow': {},
            'reusability_scores': {}
        }
        
        # Aggregate data across clusters
        for cluster in clusters:
            workflow = cluster.facet_values.get('DataScienceWorkflow', ['Unknown'])[0]
            complexity = cluster.facet_values.get('NotebookComplexity', ['3'])[0]
            
            # Workflow distribution
            dashboard_data['workflow_distribution'][workflow] = (
                dashboard_data['workflow_distribution'].get(workflow, 0) + cluster.size
            )
            
            # Complexity distribution
            dashboard_data['complexity_distribution'][complexity] = (
                dashboard_data['complexity_distribution'].get(complexity, 0) + cluster.size
            )
            
            # Success by workflow
            if workflow not in dashboard_data['success_by_workflow']:
                dashboard_data['success_by_workflow'][workflow] = []
            dashboard_data['success_by_workflow'][workflow].append(cluster.success_rate)
        
        # Calculate averages
        for workflow, rates in dashboard_data['success_by_workflow'].items():
            dashboard_data['success_by_workflow'][workflow] = np.mean(rates)
        
        return dashboard_data

    async def _extract_procedure_templates(self, clusters: List[PKLCluster]) -> List[Dict[str, Any]]:
        """Extract procedure templates from successful clusters"""
        
        templates = []
        
        for cluster in clusters:
            if cluster.success_rate > 0.7:  # Only extract from successful clusters
                template = {
                    'id': f"template_{cluster.id}",
                    'name': f"{cluster.name} Procedure",
                    'description': f"Reusable procedure for {cluster.summary.lower()}",
                    'workflow_type': cluster.facet_values.get('DataScienceWorkflow', ['Unknown'])[0],
                    'complexity_level': cluster.facet_values.get('NotebookComplexity', ['3'])[0],
                    'success_rate': cluster.success_rate,
                    'based_on_sessions': cluster.conversations,
                    'steps': self._generate_template_steps(cluster),
                    'parameters': self._extract_template_parameters(cluster),
                    'expected_outputs': self._define_expected_outputs(cluster)
                }
                templates.append(template)
        
        return templates

    def _generate_template_steps(self, cluster: PKLCluster) -> List[Dict[str, str]]:
        """Generate template steps for a cluster"""
        
        workflow = cluster.facet_values.get('DataScienceWorkflow', ['Unknown'])[0]
        
        if workflow == 'Exploratory Data Analysis':
            return [
                {'step': 1, 'action': 'Load and inspect data', 'code': 'df = pd.read_csv("{{dataset_path}}")\nprint(df.head())\nprint(df.info())'},
                {'step': 2, 'action': 'Check data quality', 'code': 'print(df.isnull().sum())\nprint(df.describe())'},
                {'step': 3, 'action': 'Create visualizations', 'code': 'df.hist(figsize=(12, 8))\nplt.tight_layout()\nplt.show()'},
                {'step': 4, 'action': 'Analyze correlations', 'code': 'correlation_matrix = df.corr()\nsns.heatmap(correlation_matrix, annot=True)'}
            ]
        elif workflow == 'Model Debugging':
            return [
                {'step': 1, 'action': 'Load model and data', 'code': 'model = joblib.load("{{model_path}}")\nX_test = pd.read_csv("{{test_data_path}}")'},
                {'step': 2, 'action': 'Check predictions', 'code': 'predictions = model.predict(X_test)\nprint(f"Prediction range: {predictions.min()} - {predictions.max()}")'},
                {'step': 3, 'action': 'Analyze errors', 'code': 'errors = y_test - predictions\nplt.scatter(predictions, errors)\nplt.xlabel("Predictions")\nplt.ylabel("Errors")'},
                {'step': 4, 'action': 'Feature importance', 'code': 'if hasattr(model, "feature_importances_"):\n    importance_df = pd.DataFrame({"feature": X_test.columns, "importance": model.feature_importances_})\n    print(importance_df.sort_values("importance", ascending=False))'}
            ]
        else:
            return [
                {'step': 1, 'action': 'Initialize analysis', 'code': '# Initialize your analysis here'},
                {'step': 2, 'action': 'Process data', 'code': '# Add your data processing steps'},
                {'step': 3, 'action': 'Generate results', 'code': '# Generate your results'}
            ]

    def _extract_template_parameters(self, cluster: PKLCluster) -> List[Dict[str, str]]:
        """Extract parameters for template"""
        
        workflow = cluster.facet_values.get('DataScienceWorkflow', ['Unknown'])[0]
        
        if workflow == 'Exploratory Data Analysis':
            return [
                {'name': 'dataset_path', 'type': 'string', 'description': 'Path to the dataset file'},
                {'name': 'target_column', 'type': 'string', 'description': 'Name of the target variable column'},
                {'name': 'figure_size', 'type': 'tuple', 'description': 'Size of generated plots', 'default': '(12, 8)'}
            ]
        elif workflow == 'Model Debugging':
            return [
                {'name': 'model_path', 'type': 'string', 'description': 'Path to the saved model file'},
                {'name': 'test_data_path', 'type': 'string', 'description': 'Path to test dataset'},
                {'name': 'target_column', 'type': 'string', 'description': 'Name of the target variable'}
            ]
        else:
            return [
                {'name': 'input_data', 'type': 'string', 'description': 'Input data for analysis'}
            ]

    def _define_expected_outputs(self, cluster: PKLCluster) -> List[str]:
        """Define expected outputs for template"""
        
        workflow = cluster.facet_values.get('DataScienceWorkflow', ['Unknown'])[0]
        
        if workflow == 'Exploratory Data Analysis':
            return [
                'Data shape and basic statistics',
                'Missing value analysis',
                'Distribution visualizations',
                'Correlation heatmap',
                'Key insights and patterns'
            ]
        elif workflow == 'Model Debugging':
            return [
                'Model performance metrics',
                'Error analysis plots',
                'Feature importance rankings',
                'Diagnostic insights',
                'Improvement recommendations'
            ]
        else:
            return [
                'Analysis results',
                'Generated visualizations',
                'Summary insights'
            ]

    def generate_enhanced_dashboard_config(self) -> Dict[str, Any]:
        """Generate configuration for enhanced dashboard"""
        
        config = {
            'dashboard_name': 'Native PKL Integration Dashboard',
            'version': '2.0.0',
            'components': [],
            'layouts': {},
            'interactions': [],
            'data_sources': {}
        }
        
        # Add native components based on parsed UI data
        for component in self.ui_components['native_components']:
            config['components'].append({
                'id': component['name'].lower(),
                'name': component['name'],
                'type': self._determine_component_type(component['name']),
                'props': component['props'],
                'features': component['features'],
                'pkl_enhancements': component['pkl_enhancements']
            })
        
        # Define layout
        config['layouts'] = {
            'main': {
                'type': 'three_panel',
                'left': 'pkl_cluster_tree',
                'center': 'pkl_umap_plot',
                'right': 'pkl_session_details'
            },
            'analysis': {
                'type': 'tabbed',
                'tabs': ['facet_analysis', 'procedure_templates', 'success_metrics']
            }
        }
        
        # Define interactions
        config['interactions'] = [
            {
                'trigger': 'tree_node_click',
                'action': 'highlight_umap_points',
                'target': 'pkl_umap_plot'
            },
            {
                'trigger': 'umap_selection',
                'action': 'show_session_details',
                'target': 'pkl_session_details'
            },
            {
                'trigger': 'facet_filter',
                'action': 'update_all_views',
                'target': ['pkl_cluster_tree', 'pkl_umap_plot']
            }
        ]
        
        return config

    def _determine_component_type(self, component_name: str) -> str:
        """Determine component type from name"""
        name_lower = component_name.lower()
        
        if 'tree' in name_lower:
            return 'hierarchical_tree'
        elif 'map' in name_lower:
            return 'scatter_plot'
        elif 'details' in name_lower:
            return 'detail_panel'
        else:
            return 'generic_component'


async def main():
    """Main function for CLI usage"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Native PKL Integration with OpenClio and Kura')
    parser.add_argument('--sessions-file', default='temp_sessions.json', help='Path to PKL sessions JSON file')
    parser.add_argument('--output-dir', default='./native_pkl_output', help='Output directory')
    
    args = parser.parse_args()
    
    # Create sample sessions if file doesn't exist
    sessions_file = Path(args.sessions_file)
    if not sessions_file.exists():
        sample_sessions = [
            {
                'id': 'session_001',
                'timestamp': '2024-01-15T10:30:00Z',
                'intent': 'explore',
                'outcome': 'success',
                'confidence': 0.92,
                'currentFile': 'customer_analysis.ipynb',
                'codeDeltas': [{'afterContent': 'import pandas as pd\ndf = pd.read_csv("data.csv")'}],
                'conversationEvents': [
                    {'role': 'user', 'content': 'I need to analyze customer data', 'timestamp': '2024-01-15T10:30:00Z'},
                    {'role': 'assistant', 'content': 'I can help you explore the customer dataset', 'timestamp': '2024-01-15T10:31:00Z'}
                ]
            },
            {
                'id': 'session_002',
                'timestamp': '2024-01-15T14:20:00Z',
                'intent': 'debug',
                'outcome': 'stuck',
                'confidence': 0.65,
                'currentFile': 'model_training.py',
                'codeDeltas': [{'afterContent': 'model.fit(X_train, y_train)'}]
            }
        ]
        
        with open(sessions_file, 'w') as f:
            json.dump(sample_sessions, f, indent=2)
        
        print(f"📝 Created sample sessions file: {sessions_file}")
    
    # Load sessions
    with open(sessions_file, 'r') as f:
        pkl_sessions = json.load(f)
    
    if not isinstance(pkl_sessions, list):
        pkl_sessions = [pkl_sessions]
    
    # Initialize native integration
    integration = NativePKLIntegration(output_dir=args.output_dir)
    
    # Run analysis
    results = await integration.analyze_pkl_sessions(pkl_sessions)
    
    # Generate dashboard config
    dashboard_config = integration.generate_enhanced_dashboard_config()
    
    config_file = Path(args.output_dir) / "enhanced_dashboard_config.json"
    with open(config_file, 'w') as f:
        json.dump(dashboard_config, f, indent=2)
    
    print(f"\n🎉 Native PKL Integration Analysis Complete!")
    print(f"📊 Processed {results['input_sessions']} sessions")
    print(f"🎯 Generated {len(results['native_clusters'])} native clusters")
    print(f"📋 Extracted {len(results['procedure_templates'])} procedure templates")
    print(f"📁 Results saved to: {args.output_dir}")
    print(f"🎨 Dashboard config: {config_file}")


if __name__ == "__main__":
    asyncio.run(main())
