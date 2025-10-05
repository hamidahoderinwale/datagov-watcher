"""
Timeline Visualizer: Creates charts and visualizations for time-series data
Part of Phase 1: Time-Series Foundation
"""

import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from src.analysis.time_series_manager import TimeSeriesManager

class TimelineVisualizer:
    def __init__(self, db_path: str = "datasets.db"):
        self.time_series_manager = TimeSeriesManager(db_path)
    
    def generate_timeline_charts(self, days: int = 30, agency_filter: str = None) -> Dict[str, str]:
        """Generate Chart.js compatible data for timeline visualizations"""
        
        timeline_data = self.time_series_manager.get_timeline_data(days, agency_filter)
        
        # Dataset count over time
        dataset_count_chart = {
            "type": "line",
            "data": {
                "labels": [snapshot['date'] for snapshot in timeline_data['snapshots']],
                "datasets": [
                    {
                        "label": "Total Datasets",
                        "data": [snapshot['total_datasets'] for snapshot in timeline_data['snapshots']],
                        "borderColor": "#4a90e2",
                        "backgroundColor": "rgba(74, 144, 226, 0.1)",
                        "fill": True
                    },
                    {
                        "label": "Available Datasets",
                        "data": [snapshot['available_datasets'] for snapshot in timeline_data['snapshots']],
                        "borderColor": "#7ed321",
                        "backgroundColor": "rgba(126, 211, 33, 0.1)",
                        "fill": True
                    },
                    {
                        "label": "Unavailable Datasets",
                        "data": [snapshot['unavailable_datasets'] for snapshot in timeline_data['snapshots']],
                        "borderColor": "#f5a623",
                        "backgroundColor": "rgba(245, 166, 35, 0.1)",
                        "fill": True
                    }
                ]
            },
            "options": {
                "responsive": True,
                "scales": {
                    "y": {"beginAtZero": True}
                },
                "plugins": {
                    "title": {
                        "display": True,
                        "text": f"Dataset Count Over Time ({days} days)"
                    }
                }
            }
        }
        
        # Availability rate over time
        availability_chart = {
            "type": "line",
            "data": {
                "labels": [snapshot['date'] for snapshot in timeline_data['snapshots']],
                "datasets": [
                    {
                        "label": "Availability Rate (%)",
                        "data": [snapshot['availability_rate'] for snapshot in timeline_data['snapshots']],
                        "borderColor": "#50c878",
                        "backgroundColor": "rgba(80, 200, 120, 0.1)",
                        "fill": True,
                        "tension": 0.4
                    }
                ]
            },
            "options": {
                "responsive": True,
                "scales": {
                    "y": {
                        "beginAtZero": True,
                        "max": 100
                    }
                },
                "plugins": {
                    "title": {
                        "display": True,
                        "text": f"Data Availability Rate ({days} days)"
                    }
                }
            }
        }
        
        # Data volume over time
        volume_chart = {
            "type": "bar",
            "data": {
                "labels": [snapshot['date'] for snapshot in timeline_data['snapshots']],
                "datasets": [
                    {
                        "label": "Total Rows",
                        "data": [snapshot['total_rows'] for snapshot in timeline_data['snapshots']],
                        "backgroundColor": "#4a90e2",
                        "borderColor": "#357abd",
                        "borderWidth": 1
                    },
                    {
                        "label": "Total Columns",
                        "data": [snapshot['total_columns'] for snapshot in timeline_data['snapshots']],
                        "backgroundColor": "#7ed321",
                        "borderColor": "#6bb81a",
                        "borderWidth": 1
                    }
                ]
            },
            "options": {
                "responsive": True,
                "scales": {
                    "y": {"beginAtZero": True}
                },
                "plugins": {
                    "title": {
                        "display": True,
                        "text": f"Data Volume Over Time ({days} days)"
                    }
                }
            }
        }
        
        # Change types over time
        change_chart = {
            "type": "bar",
            "data": {
                "labels": list(set([change['date'] for change in timeline_data['changes']])),
                "datasets": [
                    {
                        "label": "Content Changes",
                        "data": [sum(c['count'] for c in timeline_data['changes'] 
                                   if c['change_type'] == 'content_changed' and c['date'] == date)
                                for date in list(set([change['date'] for change in timeline_data['changes']]))],
                        "backgroundColor": "#f5a623"
                    },
                    {
                        "label": "Availability Changes",
                        "data": [sum(c['count'] for c in timeline_data['changes'] 
                                   if c['change_type'] == 'availability_changed' and c['date'] == date)
                                for date in list(set([change['date'] for change in timeline_data['changes']]))],
                        "backgroundColor": "#d0021b"
                    },
                    {
                        "label": "New Datasets",
                        "data": [sum(c['count'] for c in timeline_data['changes'] 
                                   if c['change_type'] == 'dataset_added' and c['date'] == date)
                                for date in list(set([change['date'] for change in timeline_data['changes']]))],
                        "backgroundColor": "#7ed321"
                    }
                ]
            },
            "options": {
                "responsive": True,
                "scales": {
                    "y": {"beginAtZero": True}
                },
                "plugins": {
                    "title": {
                        "display": True,
                        "text": f"Changes Over Time ({days} days)"
                    }
                }
            }
        }
        
        # Agency distribution pie chart
        agency_chart = {
            "type": "doughnut",
            "data": {
                "labels": [agency['agency'] for agency in timeline_data['agencies']],
                "datasets": [
                    {
                        "label": "Datasets by Agency",
                        "data": [agency['count'] for agency in timeline_data['agencies']],
                        "backgroundColor": [
                            "#4a90e2", "#7ed321", "#f5a623", "#d0021b", "#9013fe",
                            "#50c878", "#ff6b6b", "#4ecdc4", "#45b7d1", "#96ceb4"
                        ]
                    }
                ]
            },
            "options": {
                "responsive": True,
                "plugins": {
                    "title": {
                        "display": True,
                        "text": "Top Agencies by Dataset Count"
                    },
                    "legend": {
                        "position": "bottom"
                    }
                }
            }
        }
        
        return {
            "dataset_count": json.dumps(dataset_count_chart),
            "availability_rate": json.dumps(availability_chart),
            "data_volume": json.dumps(volume_chart),
            "changes": json.dumps(change_chart),
            "agencies": json.dumps(agency_chart)
        }
    
    def generate_summary_stats(self, days: int = 30, agency_filter: str = None) -> Dict[str, Any]:
        """Generate summary statistics for the dashboard"""
        
        timeline_data = self.time_series_manager.get_timeline_data(days, agency_filter)
        
        if not timeline_data['snapshots']:
            return {
                "total_datasets": 0,
                "availability_rate": 0,
                "total_changes": 0,
                "trend": "no_data",
                "top_agency": "N/A",
                "data_volume": 0
            }
        
        latest = timeline_data['snapshots'][0]
        previous = timeline_data['snapshots'][1] if len(timeline_data['snapshots']) > 1 else latest
        
        # Calculate trends
        dataset_trend = latest['total_datasets'] - previous['total_datasets']
        availability_trend = latest['availability_rate'] - previous['availability_rate']
        
        # Determine overall trend
        if dataset_trend > 0 and availability_trend > 0:
            trend = "improving"
        elif dataset_trend > 0 and availability_trend < 0:
            trend = "growing"
        elif dataset_trend < 0 and availability_trend > 0:
            trend = "consolidating"
        else:
            trend = "declining"
        
        return {
            "total_datasets": latest['total_datasets'],
            "availability_rate": round(latest['availability_rate'], 1),
            "total_changes": sum(change['count'] for change in timeline_data['changes']),
            "trend": trend,
            "dataset_trend": f"{dataset_trend:+d}",
            "availability_trend": f"{availability_trend:+.1f}%",
            "top_agency": timeline_data['agencies'][0]['agency'] if timeline_data['agencies'] else "N/A",
            "data_volume": latest['total_rows'],
            "avg_file_size": round(latest['avg_file_size'], 0)
        }
    
    def get_recent_changes(self, limit: int = 10) -> List[Dict]:
        """Get recent changes for the dashboard"""
        
        conn = self.time_series_manager.db_path
        import sqlite3
        
        conn = sqlite3.connect(conn)
        cursor = conn.cursor()
        
        # Check if dataset_changes table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='dataset_changes'")
        changes_table_exists = cursor.fetchone() is not None
        
        changes = []
        if changes_table_exists:
            cursor.execute('''
                SELECT dc.dataset_id, dc.created_at, dc.change_type, dc.change_description,
                       dc.severity, dt.title, dt.agency
                FROM dataset_changes dc
                LEFT JOIN dataset_timeline dt ON dc.dataset_id = dt.dataset_id 
                    AND dc.change_date = dt.snapshot_date
                ORDER BY dc.created_at DESC
                LIMIT ?
            ''', (limit,))
            
            for row in cursor.fetchall():
                changes.append({
                    'dataset_id': row[0],
                    'date': row[1],  # Now using created_at timestamp
                    'type': row[2],
                    'description': row[3],
                    'severity': row[4],
                    'title': row[5] or 'Unknown',
                    'agency': row[6] or 'Unknown'
                })
        
        conn.close()
        return changes
    
    def generate_monthly_timeline_charts(self, months: int = 12, agency_filter: str = None) -> Dict[str, str]:
        """Generate Chart.js compatible data for monthly timeline visualizations"""
        
        timeline_data = self.time_series_manager.get_monthly_timeline_data(months, agency_filter)
        
        # Dataset count over time (monthly)
        dataset_count_chart = {
            "type": "line",
            "data": {
                "labels": [snapshot['month'] for snapshot in timeline_data['snapshots']],
                "datasets": [
                    {
                        "label": "Avg Total Datasets",
                        "data": [snapshot['avg_total_datasets'] for snapshot in timeline_data['snapshots']],
                        "borderColor": "#4a90e2",
                        "backgroundColor": "rgba(74, 144, 226, 0.1)",
                        "fill": True
                    },
                    {
                        "label": "Avg Available Datasets",
                        "data": [snapshot['avg_available_datasets'] for snapshot in timeline_data['snapshots']],
                        "borderColor": "#7ed321",
                        "backgroundColor": "rgba(126, 211, 33, 0.1)",
                        "fill": True
                    },
                    {
                        "label": "Avg Unavailable Datasets",
                        "data": [snapshot['avg_unavailable_datasets'] for snapshot in timeline_data['snapshots']],
                        "borderColor": "#f5a623",
                        "backgroundColor": "rgba(245, 166, 35, 0.1)",
                        "fill": True
                    }
                ]
            },
            "options": {
                "responsive": True,
                "scales": {
                    "y": {"beginAtZero": True}
                },
                "plugins": {
                    "title": {
                        "display": True,
                        "text": f"Monthly Dataset Count Trends ({months} months)"
                    }
                }
            }
        }
        
        # Availability rate over time (monthly)
        availability_chart = {
            "type": "line",
            "data": {
                "labels": [snapshot['month'] for snapshot in timeline_data['snapshots']],
                "datasets": [
                    {
                        "label": "Avg Availability Rate (%)",
                        "data": [snapshot['avg_availability_rate'] for snapshot in timeline_data['snapshots']],
                        "borderColor": "#50c878",
                        "backgroundColor": "rgba(80, 200, 120, 0.1)",
                        "fill": True,
                        "tension": 0.4
                    }
                ]
            },
            "options": {
                "responsive": True,
                "scales": {
                    "y": {
                        "beginAtZero": True,
                        "max": 100
                    }
                },
                "plugins": {
                    "title": {
                        "display": True,
                        "text": f"Monthly Data Availability Rate ({months} months)"
                    }
                }
            }
        }
        
        # Data volume over time (monthly)
        volume_chart = {
            "type": "bar",
            "data": {
                "labels": [snapshot['month'] for snapshot in timeline_data['snapshots']],
                "datasets": [
                    {
                        "label": "Avg Total Rows",
                        "data": [snapshot['avg_total_rows'] for snapshot in timeline_data['snapshots']],
                        "backgroundColor": "#4a90e2",
                        "borderColor": "#357abd",
                        "borderWidth": 1
                    },
                    {
                        "label": "Avg Total Columns",
                        "data": [snapshot['avg_total_columns'] for snapshot in timeline_data['snapshots']],
                        "backgroundColor": "#7ed321",
                        "borderColor": "#6bb81a",
                        "borderWidth": 1
                    }
                ]
            },
            "options": {
                "responsive": True,
                "scales": {
                    "y": {"beginAtZero": True}
                },
                "plugins": {
                    "title": {
                        "display": True,
                        "text": f"Monthly Data Volume Trends ({months} months)"
                    }
                }
            }
        }
        
        # Dataset range over time (monthly)
        range_chart = {
            "type": "line",
            "data": {
                "labels": [snapshot['month'] for snapshot in timeline_data['snapshots']],
                "datasets": [
                    {
                        "label": "Max Total Datasets",
                        "data": [snapshot['max_total_datasets'] for snapshot in timeline_data['snapshots']],
                        "borderColor": "#d0021b",
                        "backgroundColor": "rgba(208, 2, 27, 0.1)",
                        "fill": False,
                        "borderDash": [5, 5]
                    },
                    {
                        "label": "Min Total Datasets",
                        "data": [snapshot['min_total_datasets'] for snapshot in timeline_data['snapshots']],
                        "borderColor": "#f5a623",
                        "backgroundColor": "rgba(245, 166, 35, 0.1)",
                        "fill": False,
                        "borderDash": [5, 5]
                    },
                    {
                        "label": "Avg Total Datasets",
                        "data": [snapshot['avg_total_datasets'] for snapshot in timeline_data['snapshots']],
                        "borderColor": "#4a90e2",
                        "backgroundColor": "rgba(74, 144, 226, 0.1)",
                        "fill": True
                    }
                ]
            },
            "options": {
                "responsive": True,
                "scales": {
                    "y": {"beginAtZero": True}
                },
                "plugins": {
                    "title": {
                        "display": True,
                        "text": f"Monthly Dataset Range ({months} months)"
                    }
                }
            }
        }
        
        # Monthly changes
        change_chart = {
            "type": "bar",
            "data": {
                "labels": list(set([change['month'] for change in timeline_data['changes']])),
                "datasets": [
                    {
                        "label": "Content Changes",
                        "data": [sum(c['count'] for c in timeline_data['changes'] 
                                   if c['change_type'] == 'content_changed' and c['month'] == month)
                                for month in list(set([change['month'] for change in timeline_data['changes']]))],
                        "backgroundColor": "#f5a623"
                    },
                    {
                        "label": "Availability Changes",
                        "data": [sum(c['count'] for c in timeline_data['changes'] 
                                   if c['change_type'] == 'availability_changed' and c['month'] == month)
                                for month in list(set([change['month'] for change in timeline_data['changes']]))],
                        "backgroundColor": "#d0021b"
                    },
                    {
                        "label": "New Datasets",
                        "data": [sum(c['count'] for c in timeline_data['changes'] 
                                   if c['change_type'] == 'dataset_added' and c['month'] == month)
                                for month in list(set([change['month'] for change in timeline_data['changes']]))],
                        "backgroundColor": "#7ed321"
                    }
                ]
            },
            "options": {
                "responsive": True,
                "scales": {
                    "y": {"beginAtZero": True}
                },
                "plugins": {
                    "title": {
                        "display": True,
                        "text": f"Monthly Changes ({months} months)"
                    }
                }
            }
        }
        
        # Agency distribution pie chart (same as daily)
        agency_chart = {
            "type": "doughnut",
            "data": {
                "labels": [agency['agency'] for agency in timeline_data['agencies']],
                "datasets": [
                    {
                        "label": "Datasets by Agency",
                        "data": [agency['count'] for agency in timeline_data['agencies']],
                        "backgroundColor": [
                            "#4a90e2", "#7ed321", "#f5a623", "#d0021b", "#9013fe",
                            "#50c878", "#ff6b6b", "#4ecdc4", "#45b7d1", "#96ceb4"
                        ]
                    }
                ]
            },
            "options": {
                "responsive": True,
                "plugins": {
                    "title": {
                        "display": True,
                        "text": "Top Agencies by Dataset Count"
                    },
                    "legend": {
                        "position": "bottom"
                    }
                }
            }
        }
        
        return {
            "dataset_count": json.dumps(dataset_count_chart),
            "availability_rate": json.dumps(availability_chart),
            "data_volume": json.dumps(volume_chart),
            "dataset_range": json.dumps(range_chart),
            "changes": json.dumps(change_chart),
            "agencies": json.dumps(agency_chart)
        }
    
    def generate_monthly_summary_stats(self, months: int = 12, agency_filter: str = None) -> Dict[str, Any]:
        """Generate monthly summary statistics for the dashboard"""
        
        timeline_data = self.time_series_manager.get_monthly_timeline_data(months, agency_filter)
        
        if not timeline_data['snapshots']:
            return {
                "avg_total_datasets": 0,
                "avg_availability_rate": 0,
                "total_changes": 0,
                "trend": "no_data",
                "top_agency": "N/A",
                "avg_data_volume": 0,
                "monthly_growth": 0
            }
        
        latest = timeline_data['snapshots'][0]
        previous = timeline_data['snapshots'][1] if len(timeline_data['snapshots']) > 1 else latest
        
        # Calculate trends
        dataset_trend = latest['avg_total_datasets'] - previous['avg_total_datasets']
        availability_trend = latest['avg_availability_rate'] - previous['avg_availability_rate']
        
        # Calculate monthly growth rate
        monthly_growth = (dataset_trend / previous['avg_total_datasets'] * 100) if previous['avg_total_datasets'] > 0 else 0
        
        # Determine overall trend
        if dataset_trend > 0 and availability_trend > 0:
            trend = "improving"
        elif dataset_trend > 0 and availability_trend < 0:
            trend = "growing"
        elif dataset_trend < 0 and availability_trend > 0:
            trend = "consolidating"
        else:
            trend = "declining"
        
        return {
            "avg_total_datasets": round(latest['avg_total_datasets'], 1),
            "avg_availability_rate": round(latest['avg_availability_rate'], 1),
            "total_changes": sum(change['count'] for change in timeline_data['changes']),
            "trend": trend,
            "dataset_trend": f"{dataset_trend:+.1f}",
            "availability_trend": f"{availability_trend:+.1f}%",
            "top_agency": timeline_data['agencies'][0]['agency'] if timeline_data['agencies'] else "N/A",
            "avg_data_volume": round(latest['avg_total_rows'], 0),
            "avg_file_size": round(latest['avg_file_size'], 0),
            "monthly_growth": round(monthly_growth, 2),
            "snapshot_count": latest['snapshot_count']
        }