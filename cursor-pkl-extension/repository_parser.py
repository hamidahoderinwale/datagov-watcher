#!/usr/bin/env python3
"""
Repository Parser for OpenClio and Kura

This module parses the actual OpenClio and Kura repositories to extract
their core algorithms, data structures, and implementation patterns,
then creates a deep integration for the PKL Extension.
"""

import json
import ast
import re
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime


class RepositoryParser:
    """Parse OpenClio and Kura repositories to extract implementation details"""
    
    def __init__(self, base_dir: str = "."):
        self.base_dir = Path(base_dir)
        self.openclio_dir = self.base_dir / "OpenClio"
        self.kura_dir = self.base_dir / "kura"
        
        self.parsed_data = {
            'openclio': {},
            'kura': {},
            'integration_points': [],
            'algorithms': {},
            'ui_components': {},
            'data_structures': {}
        }

    def parse_repositories(self) -> Dict[str, Any]:
        """Parse both repositories and extract key information"""
        print("🔍 Parsing OpenClio and Kura repositories...")
        
        # Parse OpenClio
        if self.openclio_dir.exists():
            self.parsed_data['openclio'] = self._parse_openclio()
        else:
            print("❌ OpenClio directory not found")
        
        # Parse Kura
        if self.kura_dir.exists():
            self.parsed_data['kura'] = self._parse_kura()
        else:
            print("❌ Kura directory not found")
        
        # Extract integration points
        self._identify_integration_points()
        
        # Extract algorithms
        self._extract_algorithms()
        
        # UI components are already extracted in individual parsers
        
        return self.parsed_data

    def _parse_openclio(self) -> Dict[str, Any]:
        """Parse OpenClio repository structure and implementation"""
        openclio_data = {
            'facets': [],
            'clustering_methods': [],
            'prompts': {},
            'data_types': {},
            'algorithms': {},
            'ui_template': None
        }
        
        # Parse facets from openclio.py
        openclio_main = self.openclio_dir / "openclio" / "openclio.py"
        if openclio_main.exists():
            content = openclio_main.read_text()
            
            # Extract mainFacets definition
            facets_match = re.search(r'mainFacets = \[(.*?)\]', content, re.DOTALL)
            if facets_match:
                openclio_data['facets'] = self._parse_facets_from_text(facets_match.group(1))
        
        # Parse data types
        types_file = self.openclio_dir / "openclio" / "opencliotypes.py"
        if types_file.exists():
            openclio_data['data_types'] = self._parse_python_classes(types_file)
        
        # Parse prompts
        prompts_file = self.openclio_dir / "openclio" / "prompts.py"
        if prompts_file.exists():
            openclio_data['prompts'] = self._extract_prompts(prompts_file)
        
        # Parse clustering methods
        kmeans_file = self.openclio_dir / "openclio" / "faissKMeans.py"
        if kmeans_file.exists():
            openclio_data['clustering_methods'] = self._parse_clustering_methods(kmeans_file)
        
        # Parse UI template
        ui_template = self.openclio_dir / "openclio" / "websiteTemplate.html"
        if ui_template.exists():
            openclio_data['ui_template'] = self._parse_html_template(ui_template)
        
        return openclio_data

    def _parse_kura(self) -> Dict[str, Any]:
        """Parse Kura repository structure and implementation"""
        kura_data = {
            'conversation_types': {},
            'clustering_pipeline': [],
            'summarization_methods': {},
            'dimensionality_reduction': {},
            'ui_components': {},
            'checkpointing': {},
            'api_structure': {}
        }
        
        # Parse conversation types
        conv_types = self.kura_dir / "kura" / "types" / "conversation.py"
        if conv_types.exists():
            kura_data['conversation_types'] = self._parse_python_classes(conv_types)
        
        # Parse clustering implementation
        cluster_file = self.kura_dir / "kura" / "cluster.py"
        if cluster_file.exists():
            kura_data['clustering_pipeline'] = self._parse_clustering_pipeline(cluster_file)
        
        # Parse summarization
        summary_file = self.kura_dir / "kura" / "summarisation.py"
        if summary_file.exists():
            kura_data['summarization_methods'] = self._parse_summarization_methods(summary_file)
        
        # Parse dimensionality reduction
        dim_file = self.kura_dir / "kura" / "dimensionality.py"
        if dim_file.exists():
            kura_data['dimensionality_reduction'] = self._parse_dimensionality_methods(dim_file)
        
        # Parse UI components
        ui_dir = self.kura_dir / "ui" / "src" / "components"
        if ui_dir.exists():
            kura_data['ui_components'] = self._parse_react_components(ui_dir)
        
        # Parse checkpointing system
        checkpoint_dir = self.kura_dir / "kura" / "checkpoints"
        if checkpoint_dir.exists():
            kura_data['checkpointing'] = self._parse_checkpointing_system(checkpoint_dir)
        
        return kura_data

    def _parse_facets_from_text(self, facets_text: str) -> List[Dict[str, Any]]:
        """Parse facet definitions from OpenClio source"""
        facets = []
        
        # Extract individual Facet() definitions
        facet_pattern = r'Facet\((.*?)\)(?=,\s*Facet|\s*\])'
        facet_matches = re.findall(facet_pattern, facets_text, re.DOTALL)
        
        for match in facet_matches:
            facet_data = {}
            
            # Extract name
            name_match = re.search(r'name="([^"]+)"', match)
            if name_match:
                facet_data['name'] = name_match.group(1)
            
            # Extract question
            question_match = re.search(r'question="([^"]+)"', match)
            if question_match:
                facet_data['question'] = question_match.group(1)
            
            # Extract prefill
            prefill_match = re.search(r'prefill="([^"]+)"', match)
            if prefill_match:
                facet_data['prefill'] = prefill_match.group(1)
            
            # Extract summaryCriteria
            criteria_match = re.search(r'summaryCriteria="([^"]+)"', match)
            if criteria_match:
                facet_data['summaryCriteria'] = criteria_match.group(1)
            
            # Extract numeric range
            numeric_match = re.search(r'numeric=\((\d+),\s*(\d+)\)', match)
            if numeric_match:
                facet_data['numeric'] = [int(numeric_match.group(1)), int(numeric_match.group(2))]
            
            facets.append(facet_data)
        
        return facets

    def _parse_python_classes(self, file_path: Path) -> Dict[str, Any]:
        """Parse Python classes and their structure"""
        try:
            content = file_path.read_text()
            tree = ast.parse(content)
            
            classes = {}
            for node in ast.walk(tree):
                if isinstance(node, ast.ClassDef):
                    class_info = {
                        'name': node.name,
                        'methods': [],
                        'attributes': [],
                        'docstring': ast.get_docstring(node)
                    }
                    
                    for item in node.body:
                        if isinstance(item, ast.FunctionDef):
                            class_info['methods'].append({
                                'name': item.name,
                                'args': [arg.arg for arg in item.args.args],
                                'docstring': ast.get_docstring(item)
                            })
                        elif isinstance(item, ast.AnnAssign) and hasattr(item.target, 'id'):
                            class_info['attributes'].append(item.target.id)
                    
                    classes[node.name] = class_info
            
            return classes
        except Exception as e:
            print(f"Error parsing {file_path}: {e}")
            return {}

    def _extract_prompts(self, file_path: Path) -> Dict[str, str]:
        """Extract prompt templates from OpenClio"""
        content = file_path.read_text()
        prompts = {}
        
        # Find all function definitions that return prompts
        prompt_functions = re.findall(r'def (get\w*Prompt.*?)\(.*?\):(.*?)(?=def|\Z)', content, re.DOTALL)
        
        for func_name, func_body in prompt_functions:
            # Extract string literals that look like prompts
            string_matches = re.findall(r'"""(.*?)"""', func_body, re.DOTALL)
            if string_matches:
                prompts[func_name] = string_matches[0].strip()
            else:
                # Try single quotes
                string_matches = re.findall(r"'''(.*?)'''", func_body, re.DOTALL)
                if string_matches:
                    prompts[func_name] = string_matches[0].strip()
        
        return prompts

    def _parse_clustering_methods(self, file_path: Path) -> List[Dict[str, Any]]:
        """Parse clustering methods from OpenClio"""
        content = file_path.read_text()
        methods = []
        
        # Look for class definitions
        classes = re.findall(r'class (\w+).*?:(.*?)(?=class|\Z)', content, re.DOTALL)
        
        for class_name, class_body in classes:
            method_info = {
                'name': class_name,
                'methods': [],
                'type': 'clustering'
            }
            
            # Extract method definitions
            method_matches = re.findall(r'def (\w+)\(.*?\):', class_body)
            method_info['methods'] = method_matches
            
            methods.append(method_info)
        
        return methods

    def _parse_html_template(self, file_path: Path) -> Dict[str, Any]:
        """Parse OpenClio HTML template structure"""
        content = file_path.read_text()
        
        template_info = {
            'has_tree_view': 'tree' in content.lower(),
            'has_umap_plot': 'umap' in content.lower() or 'plot' in content.lower(),
            'has_search': 'search' in content.lower(),
            'has_filtering': 'filter' in content.lower(),
            'javascript_libraries': [],
            'css_classes': []
        }
        
        # Extract JavaScript libraries
        js_libs = re.findall(r'<script.*?src="([^"]+)"', content)
        template_info['javascript_libraries'] = js_libs
        
        # Extract CSS classes
        css_classes = re.findall(r'class="([^"]+)"', content)
        template_info['css_classes'] = list(set(css_classes))
        
        return template_info

    def _parse_clustering_pipeline(self, file_path: Path) -> List[Dict[str, Any]]:
        """Parse Kura clustering pipeline"""
        content = file_path.read_text()
        pipeline_steps = []
        
        # Look for async function definitions
        async_funcs = re.findall(r'async def (\w+)\((.*?)\):(.*?)(?=async def|\Z)', content, re.DOTALL)
        
        for func_name, args, body in async_funcs:
            step_info = {
                'name': func_name,
                'args': args.strip(),
                'is_async': True,
                'description': self._extract_docstring(body)
            }
            pipeline_steps.append(step_info)
        
        return pipeline_steps

    def _parse_summarization_methods(self, file_path: Path) -> Dict[str, Any]:
        """Parse Kura summarization methods"""
        content = file_path.read_text()
        
        # Extract DEFAULT_SUMMARY_PROMPT
        prompt_match = re.search(r'DEFAULT_SUMMARY_PROMPT = """(.*?)"""', content, re.DOTALL)
        default_prompt = prompt_match.group(1) if prompt_match else ""
        
        return {
            'default_prompt': default_prompt,
            'methods': self._extract_function_signatures(content)
        }

    def _parse_dimensionality_methods(self, file_path: Path) -> Dict[str, Any]:
        """Parse Kura dimensionality reduction methods"""
        content = file_path.read_text()
        
        return {
            'methods': self._extract_function_signatures(content),
            'classes': self._extract_class_names(content)
        }

    def _parse_react_components(self, ui_dir: Path) -> Dict[str, Any]:
        """Parse React UI components from Kura"""
        components = {}
        
        for tsx_file in ui_dir.glob("*.tsx"):
            component_name = tsx_file.stem
            content = tsx_file.read_text()
            
            components[component_name] = {
                'name': component_name,
                'props': self._extract_react_props(content),
                'imports': self._extract_react_imports(content),
                'has_state': 'useState' in content,
                'has_effects': 'useEffect' in content
            }
        
        return components

    def _parse_checkpointing_system(self, checkpoint_dir: Path) -> Dict[str, Any]:
        """Parse Kura checkpointing system"""
        checkpointing = {
            'managers': [],
            'formats': []
        }
        
        for py_file in checkpoint_dir.glob("*.py"):
            if py_file.name == "__init__.py":
                continue
                
            content = py_file.read_text()
            
            # Extract class names
            classes = self._extract_class_names(content)
            checkpointing['managers'].extend(classes)
            
            # Determine format from filename
            if 'json' in py_file.name:
                checkpointing['formats'].append('JSONL')
            elif 'parquet' in py_file.name:
                checkpointing['formats'].append('Parquet')
            elif 'sql' in py_file.name:
                checkpointing['formats'].append('SQL')
        
        return checkpointing

    def _extract_docstring(self, text: str) -> str:
        """Extract docstring from function body"""
        docstring_match = re.search(r'"""(.*?)"""', text, re.DOTALL)
        return docstring_match.group(1).strip() if docstring_match else ""

    def _extract_function_signatures(self, content: str) -> List[str]:
        """Extract function signatures from Python code"""
        return re.findall(r'def (\w+)\([^)]*\):', content)

    def _extract_class_names(self, content: str) -> List[str]:
        """Extract class names from Python code"""
        return re.findall(r'class (\w+).*?:', content)

    def _extract_react_props(self, content: str) -> List[str]:
        """Extract React component props"""
        props_match = re.search(r'type Props = \{(.*?)\}', content, re.DOTALL)
        if props_match:
            props_text = props_match.group(1)
            return re.findall(r'(\w+):', props_text)
        return []

    def _extract_react_imports(self, content: str) -> List[str]:
        """Extract React imports"""
        imports = re.findall(r'import.*?from ["\']([^"\']+)["\']', content)
        return imports

    def _identify_integration_points(self):
        """Identify key integration points between OpenClio and Kura"""
        integration_points = []
        
        # Conversation format compatibility
        if ('conversation_types' in self.parsed_data['kura'] and 
            'data_types' in self.parsed_data['openclio']):
            integration_points.append({
                'type': 'data_format',
                'description': 'Both use conversation-based data structures',
                'kura_component': 'Conversation class',
                'openclio_component': 'ConversationFacetData',
                'integration_method': 'Format conversion layer'
            })
        
        # Clustering approaches
        if ('clustering_pipeline' in self.parsed_data['kura'] and
            'clustering_methods' in self.parsed_data['openclio']):
            integration_points.append({
                'type': 'clustering',
                'description': 'Both implement hierarchical clustering',
                'kura_component': 'meta_cluster pipeline',
                'openclio_component': 'FaissKMeans clustering',
                'integration_method': 'Hybrid clustering approach'
            })
        
        # UI visualization
        if ('ui_components' in self.parsed_data['kura'] and
            'ui_template' in self.parsed_data['openclio']):
            integration_points.append({
                'type': 'visualization',
                'description': 'Both provide tree and map visualizations',
                'kura_component': 'React components',
                'openclio_component': 'HTML template',
                'integration_method': 'Enhanced dashboard combining both approaches'
            })
        
        self.parsed_data['integration_points'] = integration_points

    def _extract_algorithms(self):
        """Extract key algorithms from both repositories"""
        algorithms = {}
        
        # OpenClio algorithms
        if 'openclio' in self.parsed_data:
            openclio = self.parsed_data['openclio']
            
            if 'clustering_methods' in openclio:
                algorithms['openclio_clustering'] = {
                    'methods': openclio['clustering_methods'],
                    'description': 'FAISS-based K-means clustering with hierarchical organization'
                }
            
            if 'facets' in openclio:
                algorithms['openclio_faceting'] = {
                    'facets': openclio['facets'],
                    'description': 'Multi-faceted conversation analysis with LLM-based extraction'
                }
        
        # Kura algorithms
        if 'kura' in self.parsed_data:
            kura = self.parsed_data['kura']
            
            if 'clustering_pipeline' in kura:
                algorithms['kura_pipeline'] = {
                    'pipeline': kura['clustering_pipeline'],
                    'description': 'Async pipeline for conversation summarization and clustering'
                }
            
            if 'dimensionality_reduction' in kura:
                algorithms['kura_dimensionality'] = {
                    'methods': kura['dimensionality_reduction'],
                    'description': 'UMAP-based dimensionality reduction for visualization'
                }
        
        self.parsed_data['algorithms'] = algorithms

    def generate_pkl_integration_spec(self) -> Dict[str, Any]:
        """Generate integration specification for PKL Extension"""
        spec = {
            'integration_name': 'PKL Extension Deep Integration',
            'timestamp': datetime.now().isoformat(),
            'repositories_parsed': {
                'openclio': bool(self.parsed_data.get('openclio')),
                'kura': bool(self.parsed_data.get('kura'))
            },
            'data_science_facets': self._create_pkl_facets(),
            'clustering_strategy': self._design_clustering_strategy(),
            'ui_enhancement_plan': self._design_ui_enhancements(),
            'implementation_roadmap': self._create_implementation_roadmap()
        }
        
        return spec

    def _create_pkl_facets(self) -> List[Dict[str, Any]]:
        """Create PKL-specific facets based on OpenClio patterns"""
        pkl_facets = [
            {
                'name': 'DataScienceWorkflow',
                'question': 'What type of data science workflow is being executed?',
                'prefill': 'The data science workflow involves',
                'summaryCriteria': 'Group by workflow type: EDA, modeling, debugging, visualization, or data preprocessing',
                'pkl_specific': True
            },
            {
                'name': 'NotebookComplexity',
                'question': 'How complex is this Jupyter notebook session?',
                'prefill': 'The notebook complexity is',
                'numeric': [1, 5],
                'summaryCriteria': 'Rate complexity based on number of cells, libraries used, and analysis depth',
                'pkl_specific': True
            },
            {
                'name': 'LibraryEcosystem',
                'question': 'What data science libraries are being used?',
                'prefill': 'The primary libraries are',
                'summaryCriteria': 'Group by library ecosystem: pandas/numpy, scikit-learn, deep learning, visualization',
                'pkl_specific': True
            },
            {
                'name': 'ProcedureReusability',
                'question': 'How reusable is this data science procedure?',
                'prefill': 'The procedure reusability is',
                'numeric': [1, 5],
                'summaryCriteria': 'Assess potential for creating reusable templates',
                'pkl_specific': True
            }
        ]
        
        return pkl_facets

    def _design_clustering_strategy(self) -> Dict[str, Any]:
        """Design hybrid clustering strategy combining OpenClio and Kura"""
        strategy = {
            'approach': 'hybrid',
            'stages': [
                {
                    'stage': 1,
                    'method': 'kura_summarization',
                    'description': 'Use Kura pipeline for conversation summarization',
                    'input': 'PKL sessions converted to Kura conversations',
                    'output': 'Conversation summaries'
                },
                {
                    'stage': 2,
                    'method': 'openclio_faceting',
                    'description': 'Apply OpenClio faceted analysis',
                    'input': 'Conversation summaries',
                    'output': 'Multi-faceted conversation data'
                },
                {
                    'stage': 3,
                    'method': 'hybrid_clustering',
                    'description': 'Combine Kura clustering with OpenClio hierarchies',
                    'input': 'Faceted conversation data',
                    'output': 'Hierarchical clusters with facet organization'
                },
                {
                    'stage': 4,
                    'method': 'kura_visualization',
                    'description': 'Use Kura UMAP projection for visualization',
                    'input': 'Hierarchical clusters',
                    'output': '2D projections for interactive visualization'
                }
            ],
            'benefits': [
                'Combines best of both approaches',
                'Maintains OpenClio faceted analysis',
                'Uses Kura async pipeline efficiency',
                'Provides rich visualization capabilities'
            ]
        }
        
        return strategy

    def _design_ui_enhancements(self) -> Dict[str, Any]:
        """Design UI enhancements combining both approaches"""
        enhancements = {
            'components': [
                {
                    'name': 'HybridClusterTree',
                    'source': 'kura_cluster_tree + openclio_hierarchy',
                    'features': ['Expandable nodes', 'Facet-based organization', 'Count indicators'],
                    'enhancement': 'Add PKL-specific metadata display'
                },
                {
                    'name': 'EnhancedUMAPPlot',
                    'source': 'kura_cluster_map + openclio_plot',
                    'features': ['Interactive selection', 'Facet-based coloring', 'Zoom controls'],
                    'enhancement': 'Add procedure pattern overlays'
                },
                {
                    'name': 'FacetedAnalysisPanel',
                    'source': 'openclio_facet_display',
                    'features': ['Multi-facet view', 'Filter controls', 'Statistics'],
                    'enhancement': 'Add PKL-specific insights'
                },
                {
                    'name': 'ProcedureTemplateGenerator',
                    'source': 'new_component',
                    'features': ['Template creation', 'Parameter extraction', 'Success metrics'],
                    'enhancement': 'Core PKL Extension feature'
                }
            ],
            'layout': 'three_panel',
            'interactions': [
                'Click tree node -> highlight UMAP points',
                'Select UMAP region -> show facet analysis',
                'Filter facets -> update tree and plot',
                'Generate template -> create notebook'
            ]
        }
        
        return enhancements

    def _create_implementation_roadmap(self) -> List[Dict[str, Any]]:
        """Create implementation roadmap for deep integration"""
        roadmap = [
            {
                'phase': 1,
                'title': 'Repository Integration Setup',
                'duration': '1-2 days',
                'tasks': [
                    'Set up repository parsing infrastructure',
                    'Create data format conversion layers',
                    'Implement mock LLM interfaces for testing',
                    'Validate repository parsing accuracy'
                ],
                'deliverables': ['Repository parser', 'Format converters', 'Test suite']
            },
            {
                'phase': 2,
                'title': 'PKL-Specific Facet Implementation',
                'duration': '2-3 days',
                'tasks': [
                    'Implement PKL-specific facets',
                    'Create data science workflow classification',
                    'Build procedure complexity analysis',
                    'Test facet extraction accuracy'
                ],
                'deliverables': ['PKL facet system', 'Workflow classifier', 'Complexity analyzer']
            },
            {
                'phase': 3,
                'title': 'Hybrid Clustering Pipeline',
                'duration': '3-4 days',
                'tasks': [
                    'Implement Kura conversation processing',
                    'Integrate OpenClio faceted clustering',
                    'Build hierarchical organization',
                    'Add UMAP visualization pipeline'
                ],
                'deliverables': ['Hybrid clustering system', 'Visualization pipeline', 'Performance metrics']
            },
            {
                'phase': 4,
                'title': 'Enhanced UI Integration',
                'duration': '2-3 days',
                'tasks': [
                    'Port Kura React components to vanilla JS',
                    'Integrate OpenClio visualization patterns',
                    'Build PKL-specific UI components',
                    'Implement interactive features'
                ],
                'deliverables': ['Enhanced dashboard', 'Interactive components', 'User documentation']
            },
            {
                'phase': 5,
                'title': 'Testing and Optimization',
                'duration': '1-2 days',
                'tasks': [
                    'Comprehensive integration testing',
                    'Performance optimization',
                    'User experience validation',
                    'Documentation completion'
                ],
                'deliverables': ['Test results', 'Performance benchmarks', 'Complete documentation']
            }
        ]
        
        return roadmap

    def save_parsed_data(self, output_file: str = "repository_analysis.json"):
        """Save parsed repository data to file"""
        output_path = Path(output_file)
        
        with open(output_path, 'w') as f:
            json.dump(self.parsed_data, f, indent=2, default=str)
        
        print(f"📁 Repository analysis saved to {output_path}")

    def save_integration_spec(self, output_file: str = "pkl_integration_spec.json"):
        """Save PKL integration specification to file"""
        spec = self.generate_pkl_integration_spec()
        output_path = Path(output_file)
        
        with open(output_path, 'w') as f:
            json.dump(spec, f, indent=2, default=str)
        
        print(f"📋 Integration specification saved to {output_path}")


def main():
    """Main function for CLI usage"""
    parser = RepositoryParser()
    
    print("🚀 Starting repository parsing...")
    parsed_data = parser.parse_repositories()
    
    print(f"\n📊 Parsing Results:")
    print(f"  OpenClio facets found: {len(parsed_data.get('openclio', {}).get('facets', []))}")
    print(f"  Kura components found: {len(parsed_data.get('kura', {}).get('ui_components', {}))}")
    print(f"  Integration points: {len(parsed_data.get('integration_points', []))}")
    
    # Save results
    parser.save_parsed_data()
    parser.save_integration_spec()
    
    print("\n🎉 Repository parsing complete!")
    print("📁 Check repository_analysis.json for detailed parsing results")
    print("📋 Check pkl_integration_spec.json for integration specifications")


if __name__ == "__main__":
    main()
