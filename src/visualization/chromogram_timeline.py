"""
Chromogram Timeline Visualizer
Implements the core Chromogram visualization from the Dataset State Historian plan
"""

import json
import sqlite3
import hashlib
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from pathlib import Path
import pandas as pd

logger = logging.getLogger(__name__)

class ChromogramTimeline:
    def __init__(self, db_path: str = "datasets.db"):
        self.db_path = db_path
    
    def generate_chromogram_data(self, dataset_id: str, days: int = 30) -> Dict:
        """Generate Chromogram-style timeline data for a dataset"""
        logger.info(f"Generating Chromogram timeline for {dataset_id}")
        
        # Get timeline data
        timeline_data = self._get_timeline_data(dataset_id, days)
        if not timeline_data:
            return {'error': 'No timeline data available'}
        
        # Generate field groups
        field_groups = self._generate_field_groups(timeline_data)
        
        # Generate Chromogram encoding
        chromogram_data = self._generate_chromogram_encoding(timeline_data, field_groups)
        
        # Generate interaction data
        interaction_data = self._generate_interaction_data(timeline_data, field_groups)
        
        return {
            'dataset_id': dataset_id,
            'timeline_days': days,
            'field_groups': field_groups,
            'chromogram_data': chromogram_data,
            'interaction_data': interaction_data,
            'summary': self._generate_timeline_summary(timeline_data)
        }
    
    def _get_timeline_data(self, dataset_id: str, days: int) -> List[Dict]:
        """Get timeline data for dataset"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get snapshots within date range
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        cursor.execute('''
            SELECT snapshot_date, title, agency, url, status_code, 
                   content_hash, file_size, content_type, resource_format,
                   row_count, column_count, schema, last_modified, availability
            FROM dataset_states 
            WHERE dataset_id = ? AND snapshot_date >= ?
            ORDER BY snapshot_date ASC
        ''', (dataset_id, start_date))
        
        columns = [description[0] for description in cursor.description]
        snapshots = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        # Parse schema JSON
        for snapshot in snapshots:
            if snapshot['schema']:
                try:
                    snapshot['schema'] = json.loads(snapshot['schema'])
                except:
                    snapshot['schema'] = {}
        
        conn.close()
        return snapshots
    
    def _generate_field_groups(self, timeline_data: List[Dict]) -> Dict:
        """Generate field groups for Chromogram visualization"""
        field_groups = {
            'schema': {
                'name': 'Schema',
                'fields': [],
                'color_scheme': 'blue'
            },
            'metadata': {
                'name': 'Metadata',
                'fields': ['title', 'agency', 'url', 'license', 'publisher'],
                'color_scheme': 'green'
            },
            'content': {
                'name': 'Content',
                'fields': ['row_count', 'column_count', 'file_size', 'content_similarity'],
                'color_scheme': 'orange'
            }
        }
        
        # Extract schema fields from all snapshots
        all_columns = set()
        for snapshot in timeline_data:
            schema = snapshot.get('schema', {})
            columns = schema.get('columns', [])
            all_columns.update(columns)
        
        field_groups['schema']['fields'] = list(all_columns)
        
        return field_groups
    
    def _generate_chromogram_encoding(self, timeline_data: List[Dict], field_groups: Dict) -> Dict:
        """Generate Chromogram encoding data"""
        chromogram_data = {
            'timeline': [],
            'field_encodings': {},
            'change_matrix': []
        }
        
        # Generate timeline points
        for i, snapshot in enumerate(timeline_data):
            timeline_point = {
                'date': snapshot['snapshot_date'],
                'index': i,
                'fields': {}
            }
            
            # Process each field group
            for group_name, group_info in field_groups.items():
                timeline_point['fields'][group_name] = {}
                
                for field in group_info['fields']:
                    value = self._extract_field_value(snapshot, field)
                    encoding = self._generate_field_encoding(field, value, group_info['color_scheme'])
                    
                    timeline_point['fields'][group_name][field] = {
                        'value': value,
                        'encoding': encoding,
                        'changed': self._detect_field_change(timeline_data, i, field)
                    }
            
            chromogram_data['timeline'].append(timeline_point)
        
        # Generate field encodings
        for group_name, group_info in field_groups.items():
            for field in group_info['fields']:
                chromogram_data['field_encodings'][field] = {
                    'group': group_name,
                    'color_scheme': group_info['color_scheme'],
                    'chromogram_code': self._generate_chromogram_code(field)
                }
        
        # Generate change matrix
        chromogram_data['change_matrix'] = self._generate_change_matrix(timeline_data, field_groups)
        
        return chromogram_data
    
    def _extract_field_value(self, snapshot: Dict, field: str) -> Any:
        """Extract field value from snapshot"""
        if field in snapshot:
            return snapshot[field]
        
        # Check in schema
        schema = snapshot.get('schema', {})
        if field in schema:
            return schema[field]
        
        # Check in manifest (if available)
        manifest = snapshot.get('manifest', {})
        if field in manifest:
            return manifest[field]
        
        return None
    
    def _generate_field_encoding(self, field: str, value: Any, color_scheme: str) -> Dict:
        """Generate visual encoding for a field"""
        # Generate 3-char Chromogram code
        chromogram_code = self._generate_chromogram_code(field)
        
        # Determine brightness based on value
        brightness = self._calculate_brightness(value)
        
        # Generate color based on scheme
        color = self._generate_color(chromogram_code, color_scheme, brightness)
        
        return {
            'chromogram_code': chromogram_code,
            'color': color,
            'brightness': brightness,
            'value_type': self._classify_value_type(value)
        }
    
    def _generate_chromogram_code(self, field: str) -> str:
        """Generate 3-character Chromogram code from field name"""
        # Use first 3 characters, padded if necessary
        code = field[:3].upper()
        if len(code) < 3:
            code = code.ljust(3, 'X')
        
        return code
    
    def _calculate_brightness(self, value: Any) -> float:
        """Calculate brightness based on value"""
        if value is None:
            return 0.3
        
        if isinstance(value, (int, float)):
            # Normalize numeric values
            if value == 0:
                return 0.5
            return min(1.0, max(0.1, abs(value) / 1000))
        
        if isinstance(value, str):
            # Brightness based on string length and content
            if not value:
                return 0.3
            return min(1.0, len(value) / 50)
        
        if isinstance(value, bool):
            return 1.0 if value else 0.3
        
        return 0.7
    
    def _generate_color(self, chromogram_code: str, color_scheme: str, brightness: float) -> str:
        """Generate color from Chromogram code and scheme"""
        # Map color schemes to base colors
        scheme_colors = {
            'blue': '#4A90E2',
            'green': '#7ED321',
            'orange': '#F5A623',
            'red': '#D0021B',
            'purple': '#9013FE'
        }
        
        base_color = scheme_colors.get(color_scheme, '#4A90E2')
        
        # Adjust brightness
        # Convert hex to RGB, adjust brightness, convert back
        hex_color = base_color.lstrip('#')
        rgb = tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))
        
        # Apply brightness
        adjusted_rgb = tuple(int(c * brightness) for c in rgb)
        
        return f"rgb({adjusted_rgb[0]}, {adjusted_rgb[1]}, {adjusted_rgb[2]})"
    
    def _classify_value_type(self, value: Any) -> str:
        """Classify the type of value"""
        if value is None:
            return 'null'
        elif isinstance(value, bool):
            return 'boolean'
        elif isinstance(value, (int, float)):
            return 'numeric'
        elif isinstance(value, str):
            return 'text'
        else:
            return 'unknown'
    
    def _detect_field_change(self, timeline_data: List[Dict], current_index: int, field: str) -> bool:
        """Detect if field changed from previous snapshot"""
        if current_index == 0:
            return False
        
        current_value = self._extract_field_value(timeline_data[current_index], field)
        previous_value = self._extract_field_value(timeline_data[current_index - 1], field)
        
        return current_value != previous_value
    
    def _generate_change_matrix(self, timeline_data: List[Dict], field_groups: Dict) -> List[List[bool]]:
        """Generate change matrix for all fields across timeline"""
        matrix = []
        
        for i, snapshot in enumerate(timeline_data):
            row = []
            
            for group_name, group_info in field_groups.items():
                for field in group_info['fields']:
                    changed = self._detect_field_change(timeline_data, i, field)
                    row.append(changed)
            
            matrix.append(row)
        
        return matrix
    
    def _generate_interaction_data(self, timeline_data: List[Dict], field_groups: Dict) -> Dict:
        """Generate interaction data for tooltips and clicks"""
        interaction_data = {
            'tooltips': [],
            'field_diffs': {},
            'drill_down_data': {}
        }
        
        # Generate tooltip data
        for i, snapshot in enumerate(timeline_data):
            tooltip = {
                'date': snapshot['snapshot_date'],
                'index': i,
                'fields': {}
            }
            
            for group_name, group_info in field_groups.items():
                tooltip['fields'][group_name] = {}
                
                for field in group_info['fields']:
                    value = self._extract_field_value(snapshot, field)
                    tooltip['fields'][group_name][field] = {
                        'value': value,
                        'formatted_value': self._format_value_for_display(value)
                    }
            
            interaction_data['tooltips'].append(tooltip)
        
        # Generate field diff data
        for i in range(1, len(timeline_data)):
            prev_snapshot = timeline_data[i-1]
            curr_snapshot = timeline_data[i]
            
            diff_key = f"{prev_snapshot['snapshot_date']}__{curr_snapshot['snapshot_date']}"
            interaction_data['field_diffs'][diff_key] = {}
            
            for group_name, group_info in field_groups.items():
                interaction_data['field_diffs'][diff_key][group_name] = {}
                
                for field in group_info['fields']:
                    old_value = self._extract_field_value(prev_snapshot, field)
                    new_value = self._extract_field_value(curr_snapshot, field)
                    
                    if old_value != new_value:
                        interaction_data['field_diffs'][diff_key][group_name][field] = {
                            'old_value': old_value,
                            'new_value': new_value,
                            'change_type': self._classify_change_type(old_value, new_value)
                        }
        
        return interaction_data
    
    def _format_value_for_display(self, value: Any) -> str:
        """Format value for display in tooltips"""
        if value is None:
            return 'N/A'
        elif isinstance(value, (int, float)):
            if isinstance(value, float):
                return f"{value:.2f}"
            return f"{value:,}"
        elif isinstance(value, str):
            if len(value) > 50:
                return value[:47] + "..."
            return value
        else:
            return str(value)
    
    def _classify_change_type(self, old_value: Any, new_value: Any) -> str:
        """Classify the type of change"""
        if old_value is None and new_value is not None:
            return 'added'
        elif old_value is not None and new_value is None:
            return 'removed'
        elif isinstance(old_value, (int, float)) and isinstance(new_value, (int, float)):
            if new_value > old_value:
                return 'increased'
            else:
                return 'decreased'
        else:
            return 'modified'
    
    def _generate_timeline_summary(self, timeline_data: List[Dict]) -> Dict:
        """Generate timeline summary statistics"""
        if not timeline_data:
            return {}
        
        # Calculate basic stats
        total_snapshots = len(timeline_data)
        date_range = {
            'start': timeline_data[0]['snapshot_date'],
            'end': timeline_data[-1]['snapshot_date']
        }
        
        # Calculate change frequency
        total_changes = 0
        for i in range(1, len(timeline_data)):
            snapshot = timeline_data[i]
            prev_snapshot = timeline_data[i-1]
            
            # Count changed fields
            for field in ['title', 'agency', 'url', 'row_count', 'column_count']:
                if snapshot.get(field) != prev_snapshot.get(field):
                    total_changes += 1
        
        change_frequency = total_changes / (total_snapshots - 1) if total_snapshots > 1 else 0
        
        # Calculate volatility
        volatility_score = min(1.0, change_frequency / 10)  # Normalize to 0-1
        
        return {
            'total_snapshots': total_snapshots,
            'date_range': date_range,
            'change_frequency': change_frequency,
            'volatility_score': volatility_score,
            'risk_level': self._assess_risk_level(volatility_score)
        }
    
    def _assess_risk_level(self, volatility_score: float) -> str:
        """Assess risk level based on volatility score"""
        if volatility_score > 0.7:
            return 'high'
        elif volatility_score > 0.4:
            return 'medium'
        else:
            return 'low'
    
    def generate_chromogram_html(self, dataset_id: str, days: int = 30) -> str:
        """Generate HTML for Chromogram visualization"""
        data = self.generate_chromogram_data(dataset_id, days)
        
        html_template = """
<!DOCTYPE html>
<html>
<head>
    <title>Chromogram Timeline - {dataset_id}</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        .chromogram-container {{
            width: 100%;
            height: 600px;
            border: 1px solid #ddd;
            margin: 20px 0;
        }}
        .field-group {{
            margin: 10px 0;
            padding: 10px;
            border: 1px solid #eee;
            border-radius: 5px;
        }}
        .field-group h3 {{
            margin: 0 0 10px 0;
            color: #333;
        }}
        .field-row {{
            display: flex;
            align-items: center;
            margin: 5px 0;
        }}
        .field-name {{
            width: 150px;
            font-weight: bold;
            margin-right: 10px;
        }}
        .timeline-cells {{
            display: flex;
            gap: 2px;
        }}
        .timeline-cell {{
            width: 20px;
            height: 20px;
            border: 1px solid #ccc;
            cursor: pointer;
            position: relative;
        }}
        .timeline-cell:hover {{
            border: 2px solid #000;
        }}
        .tooltip {{
            position: absolute;
            background: #333;
            color: white;
            padding: 5px;
            border-radius: 3px;
            font-size: 12px;
            z-index: 1000;
            display: none;
        }}
    </style>
</head>
<body>
    <h1>Chromogram Timeline: {dataset_id}</h1>
    <div class="chromogram-container" id="chromogram-container">
        <!-- Chromogram will be rendered here -->
    </div>
    
    <script>
        const chromogramData = {data};
        
        function renderChromogram() {{
            const container = document.getElementById('chromogram-container');
            
            // Render field groups
            for (const [groupName, groupData] of Object.entries(chromogramData.field_groups)) {{
                const groupDiv = document.createElement('div');
                groupDiv.className = 'field-group';
                groupDiv.innerHTML = `
                    <h3>${{groupData.name}}</h3>
                    <div id="group-${{groupName}}"></div>
                `;
                container.appendChild(groupDiv);
                
                // Render fields in this group
                const groupContainer = groupDiv.querySelector(`#group-${{groupName}}`);
                for (const field of groupData.fields) {{
                    const fieldRow = document.createElement('div');
                    fieldRow.className = 'field-row';
                    
                    const fieldName = document.createElement('div');
                    fieldName.className = 'field-name';
                    fieldName.textContent = field;
                    fieldRow.appendChild(fieldName);
                    
                    const timelineCells = document.createElement('div');
                    timelineCells.className = 'timeline-cells';
                    
                    // Render timeline cells
                    for (let i = 0; i < chromogramData.timeline.length; i++) {{
                        const cell = document.createElement('div');
                        cell.className = 'timeline-cell';
                        
                        const timelinePoint = chromogramData.timeline[i];
                        const fieldData = timelinePoint.fields[groupName][field];
                        
                        if (fieldData) {{
                            cell.style.backgroundColor = fieldData.encoding.color;
                            cell.style.opacity = fieldData.encoding.brightness;
                            
                            if (fieldData.changed) {{
                                cell.style.border = '2px solid #ff0000';
                            }}
                            
                            // Add tooltip
                            cell.title = `${{field}}: ${{fieldData.value}} (${{timelinePoint.date}})`;
                        }}
                        
                        timelineCells.appendChild(cell);
                    }}
                    
                    fieldRow.appendChild(timelineCells);
                    groupContainer.appendChild(fieldRow);
                }}
            }}
        }}
        
        // Initialize
        renderChromogram();
    </script>
</body>
</html>
        """
        
        return html_template.format(
            dataset_id=dataset_id,
            data=json.dumps(data, indent=2)
        )
    
    def save_chromogram_html(self, dataset_id: str, days: int = 30, output_path: str = None) -> str:
        """Save Chromogram HTML to file"""
        html_content = self.generate_chromogram_html(dataset_id, days)
        
        if not output_path:
            output_path = f"chromogram_{dataset_id}_{days}days.html"
        
        with open(output_path, 'w') as f:
            f.write(html_content)
        
        logger.info(f"Chromogram HTML saved to {output_path}")
        return output_path

def main():
    """Test Chromogram timeline"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Chromogram Timeline Test')
    parser.add_argument('--dataset-id', required=True, help='Dataset ID')
    parser.add_argument('--days', type=int, default=30, help='Number of days')
    parser.add_argument('--output', help='Output HTML file path')
    
    args = parser.parse_args()
    
    logging.basicConfig(level=logging.INFO)
    
    timeline = ChromogramTimeline()
    
    # Generate and save HTML
    output_path = timeline.save_chromogram_html(
        args.dataset_id, 
        args.days, 
        args.output
    )
    
    print(f"Chromogram timeline generated: {output_path}")

if __name__ == '__main__':
    main()
