#!/usr/bin/env python3
"""
Test script for PKL-Kura integration

This script creates sample PKL session data and tests the Kura integration bridge.
"""

import asyncio
import json
import tempfile
import os
from datetime import datetime, timedelta
from pathlib import Path

# Sample PKL session data for testing
SAMPLE_PKL_SESSIONS = [
    {
        "id": "session_001",
        "timestamp": "2024-01-15T10:30:00Z",
        "endTime": "2024-01-15T11:15:00Z",
        "intent": "explore",
        "phase": "success",
        "outcome": "success",
        "confidence": 0.92,
        "currentFile": "data_analysis.ipynb",
        "conversationEvents": [
            {
                "id": "conv_001",
                "sessionId": "session_001",
                "timestamp": "2024-01-15T10:30:00Z",
                "role": "user",
                "content": "I need to analyze this customer dataset and find patterns in purchasing behavior",
                "referencedFiles": ["customer_data.csv"],
                "codeBlocks": []
            },
            {
                "id": "conv_002", 
                "sessionId": "session_001",
                "timestamp": "2024-01-15T10:32:00Z",
                "role": "assistant",
                "content": "I'll help you analyze the customer dataset. Let's start by loading the data and doing some initial exploration.",
                "referencedFiles": [],
                "codeBlocks": [
                    {
                        "language": "python",
                        "content": "import pandas as pd\nimport numpy as np\nimport matplotlib.pyplot as plt\n\ndf = pd.read_csv('customer_data.csv')\nprint(df.head())\nprint(df.info())"
                    }
                ]
            },
            {
                "id": "conv_003",
                "sessionId": "session_001", 
                "timestamp": "2024-01-15T10:45:00Z",
                "role": "user",
                "content": "Great! I can see we have purchase amounts, dates, and customer demographics. Can you help me identify high-value customers?",
                "referencedFiles": [],
                "codeBlocks": []
            },
            {
                "id": "conv_004",
                "sessionId": "session_001",
                "timestamp": "2024-01-15T10:47:00Z", 
                "role": "assistant",
                "content": "Absolutely! Let's segment customers by their total purchase value and frequency.",
                "referencedFiles": [],
                "codeBlocks": [
                    {
                        "language": "python",
                        "content": "# Calculate customer metrics\ncustomer_metrics = df.groupby('customer_id').agg({\n    'purchase_amount': ['sum', 'mean', 'count'],\n    'purchase_date': ['min', 'max']\n}).round(2)\n\n# Identify high-value customers (top 20%)\nhigh_value_threshold = customer_metrics[('purchase_amount', 'sum')].quantile(0.8)\nhigh_value_customers = customer_metrics[customer_metrics[('purchase_amount', 'sum')] >= high_value_threshold]\n\nprint(f'High-value customers: {len(high_value_customers)}')\nprint(high_value_customers.head())"
                    }
                ]
            }
        ],
        "fileChanges": [
            {
                "id": "fc_001",
                "sessionId": "session_001",
                "timestamp": "2024-01-15T10:35:00Z",
                "filePath": "data_analysis.ipynb",
                "changeType": "modified",
                "beforeSnippet": "# Cell 1\n",
                "afterSnippet": "# Cell 1\nimport pandas as pd\nimport numpy as np",
                "lineRange": {"start": 1, "end": 3}
            }
        ],
        "codeDeltas": [
            {
                "id": "cd_001",
                "sessionId": "session_001",
                "timestamp": "2024-01-15T10:35:00Z",
                "filePath": "data_analysis.ipynb", 
                "beforeContent": "",
                "afterContent": "import pandas as pd\nimport numpy as np\nimport matplotlib.pyplot as plt",
                "diff": "+import pandas as pd\n+import numpy as np\n+import matplotlib.pyplot as plt",
                "changeType": "added",
                "lineCount": 3
            }
        ],
        "linkedEvents": [
            {
                "id": "le_001",
                "sessionId": "session_001",
                "timestamp": "2024-01-15T10:50:00Z",
                "type": "code_run",
                "filePath": "data_analysis.ipynb",
                "output": "Successfully identified 145 high-value customers",
                "tag": "analysis",
                "classification": "success"
            }
        ],
        "privacyMode": False,
        "userConsent": True,
        "annotations": []
    },
    {
        "id": "session_002",
        "timestamp": "2024-01-15T14:20:00Z",
        "endTime": "2024-01-15T14:45:00Z", 
        "intent": "debug",
        "phase": "stuck",
        "outcome": "stuck",
        "confidence": 0.65,
        "currentFile": "model_training.py",
        "conversationEvents": [
            {
                "id": "conv_005",
                "sessionId": "session_002",
                "timestamp": "2024-01-15T14:20:00Z",
                "role": "user",
                "content": "My machine learning model is giving me very poor accuracy. The training loss isn't decreasing.",
                "referencedFiles": ["model_training.py", "dataset.csv"],
                "codeBlocks": []
            },
            {
                "id": "conv_006",
                "sessionId": "session_002",
                "timestamp": "2024-01-15T14:22:00Z",
                "role": "assistant", 
                "content": "Let's debug this step by step. First, let's check your data preprocessing and model architecture.",
                "referencedFiles": [],
                "codeBlocks": [
                    {
                        "language": "python",
                        "content": "# Check data shape and distribution\nprint('Data shape:', X_train.shape, X_test.shape)\nprint('Target distribution:', np.bincount(y_train))\nprint('Feature ranges:', X_train.min(), X_train.max())"
                    }
                ]
            },
            {
                "id": "conv_007",
                "sessionId": "session_002",
                "timestamp": "2024-01-15T14:35:00Z",
                "role": "user",
                "content": "I see the issue - my features aren't normalized and there's a class imbalance.",
                "referencedFiles": [],
                "codeBlocks": []
            }
        ],
        "fileChanges": [
            {
                "id": "fc_002",
                "sessionId": "session_002",
                "timestamp": "2024-01-15T14:30:00Z",
                "filePath": "model_training.py",
                "changeType": "modified",
                "beforeSnippet": "X_train, X_test = train_test_split(X, y)",
                "afterSnippet": "# Normalize features\nscaler = StandardScaler()\nX_scaled = scaler.fit_transform(X)\nX_train, X_test = train_test_split(X_scaled, y)",
                "lineRange": {"start": 25, "end": 28}
            }
        ],
        "codeDeltas": [
            {
                "id": "cd_002",
                "sessionId": "session_002",
                "timestamp": "2024-01-15T14:30:00Z",
                "filePath": "model_training.py",
                "beforeContent": "X_train, X_test = train_test_split(X, y)",
                "afterContent": "scaler = StandardScaler()\nX_scaled = scaler.fit_transform(X)\nX_train, X_test = train_test_split(X_scaled, y)",
                "diff": "-X_train, X_test = train_test_split(X, y)\n+scaler = StandardScaler()\n+X_scaled = scaler.fit_transform(X)\n+X_train, X_test = train_test_split(X_scaled, y)",
                "changeType": "modified",
                "lineCount": 3
            }
        ],
        "linkedEvents": [
            {
                "id": "le_002",
                "sessionId": "session_002",
                "timestamp": "2024-01-15T14:40:00Z",
                "type": "error",
                "filePath": "model_training.py",
                "output": "Still getting poor accuracy after normalization",
                "tag": "debugging",
                "classification": "stuck"
            }
        ],
        "privacyMode": False,
        "userConsent": True,
        "annotations": [
            {
                "id": "ann_001",
                "sessionId": "session_002",
                "timestamp": "2024-01-15T14:45:00Z",
                "content": "Need to investigate class imbalance issue further",
                "tags": ["debugging", "todo"]
            }
        ]
    },
    {
        "id": "session_003",
        "timestamp": "2024-01-16T09:15:00Z",
        "endTime": "2024-01-16T10:30:00Z",
        "intent": "implement",
        "phase": "success", 
        "outcome": "success",
        "confidence": 0.88,
        "currentFile": "visualization.py",
        "conversationEvents": [
            {
                "id": "conv_008",
                "sessionId": "session_003",
                "timestamp": "2024-01-16T09:15:00Z",
                "role": "user",
                "content": "I need to create an interactive dashboard to visualize our sales data with filters and drill-down capabilities.",
                "referencedFiles": ["sales_data.csv"],
                "codeBlocks": []
            },
            {
                "id": "conv_009",
                "sessionId": "session_003",
                "timestamp": "2024-01-16T09:17:00Z",
                "role": "assistant",
                "content": "Great! Let's build an interactive dashboard using Plotly Dash. We'll create multiple visualizations with filtering capabilities.",
                "referencedFiles": [],
                "codeBlocks": [
                    {
                        "language": "python",
                        "content": "import dash\nfrom dash import dcc, html, Input, Output\nimport plotly.express as px\nimport pandas as pd\n\n# Load data\ndf = pd.read_csv('sales_data.csv')\n\n# Initialize Dash app\napp = dash.Dash(__name__)\n\n# Define layout\napp.layout = html.Div([\n    html.H1('Sales Dashboard'),\n    dcc.Dropdown(\n        id='region-dropdown',\n        options=[{'label': region, 'value': region} for region in df['region'].unique()],\n        value=df['region'].unique()[0]\n    ),\n    dcc.Graph(id='sales-chart')\n])"
                    }
                ]
            }
        ],
        "fileChanges": [
            {
                "id": "fc_003",
                "sessionId": "session_003",
                "timestamp": "2024-01-16T09:25:00Z",
                "filePath": "visualization.py",
                "changeType": "created",
                "beforeSnippet": "",
                "afterSnippet": "import dash\nfrom dash import dcc, html, Input, Output\nimport plotly.express as px",
                "lineRange": {"start": 1, "end": 3}
            }
        ],
        "codeDeltas": [
            {
                "id": "cd_003",
                "sessionId": "session_003",
                "timestamp": "2024-01-16T09:25:00Z",
                "filePath": "visualization.py",
                "beforeContent": "",
                "afterContent": "import dash\nfrom dash import dcc, html, Input, Output\nimport plotly.express as px\nimport pandas as pd\n\n# Dashboard implementation\napp = dash.Dash(__name__)",
                "diff": "+import dash\n+from dash import dcc, html, Input, Output\n+import plotly.express as px\n+import pandas as pd\n+\n+# Dashboard implementation\n+app = dash.Dash(__name__)",
                "changeType": "added",
                "lineCount": 7
            }
        ],
        "linkedEvents": [
            {
                "id": "le_003",
                "sessionId": "session_003",
                "timestamp": "2024-01-16T10:15:00Z",
                "type": "success",
                "filePath": "visualization.py",
                "output": "Dashboard successfully created and running on localhost:8050",
                "tag": "implementation",
                "classification": "success"
            }
        ],
        "privacyMode": False,
        "userConsent": True,
        "annotations": []
    }
]


async def test_kura_integration():
    """Test the Kura integration with sample data"""
    print("🧪 Testing PKL-Kura Integration")
    print("=" * 50)
    
    # Create temporary files
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Save sample sessions to JSON file
        sessions_file = temp_path / "sample_sessions.json"
        with open(sessions_file, 'w') as f:
            json.dump(SAMPLE_PKL_SESSIONS, f, indent=2)
        
        print(f"✅ Created sample sessions file: {sessions_file}")
        print(f"📊 Sample data contains {len(SAMPLE_PKL_SESSIONS)} sessions")
        
        # Test the bridge
        try:
            # Import the bridge (need to add to Python path)
            import sys
            sys.path.append(str(Path(__file__).parent))
            
            from kura_bridge import PKLKuraBridge
            
            # Initialize bridge
            cache_dir = temp_path / "kura_cache"
            output_dir = temp_path / "kura_output"
            
            bridge = PKLKuraBridge(
                cache_dir=str(cache_dir),
                output_dir=str(output_dir),
                test_mode=True  # Enable test mode to avoid API requirements
            )
            
            print("✅ Initialized PKL-Kura bridge")
            
            # Test conversion
            conversations = bridge.convert_pkl_sessions_to_conversations(SAMPLE_PKL_SESSIONS)
            print(f"✅ Converted {len(conversations)} sessions to Kura format")
            
            # Print sample conversation
            if conversations:
                sample_conv = conversations[0]
                print(f"\n📝 Sample converted conversation:")
                print(f"   ID: {sample_conv['id'] if isinstance(sample_conv, dict) else sample_conv.id}")
                print(f"   Messages: {len(sample_conv['messages'] if isinstance(sample_conv, dict) else sample_conv.messages)}")
                print(f"   Metadata: {sample_conv['metadata'] if isinstance(sample_conv, dict) else sample_conv.metadata}")
                
                messages = sample_conv['messages'] if isinstance(sample_conv, dict) else sample_conv.messages
                if messages:
                    print(f"   First message: {messages[0]['content'][:100]}...")
            
            # Test analysis in mock mode
            print(f"\n🔬 Starting Kura analysis pipeline...")
            results = await bridge.analyze_conversations(conversations)
            
            print("✅ Kura integration bridge is working correctly!")
            print(f"✅ Analysis generated {len(results.get('clusters', []))} clusters")
            
            # Test dashboard data generation
            dashboard_data = bridge.generate_dashboard_data(results)
            print(f"✅ Dashboard data generated with {len(dashboard_data.get('hierarchical_clusters', {}))} hierarchical clusters")
            
            # Show what would be analyzed
            print(f"\n📈 Analysis would process:")
            print(f"   - {len(conversations)} conversations")
            print(f"   - Intent distribution: {[(c['metadata'] if isinstance(c, dict) else c.metadata).get('intent') for c in conversations]}")
            print(f"   - Outcome distribution: {[(c['metadata'] if isinstance(c, dict) else c.metadata).get('outcome') for c in conversations]}")
            print(f"   - File types: {[Path((c['metadata'] if isinstance(c, dict) else c.metadata).get('currentFile', '')).suffix for c in conversations]}")
            
            return True
            
        except ImportError as e:
            print(f"❌ Import error: {e}")
            print("💡 Make sure Kura is installed in the virtual environment")
            return False
        except Exception as e:
            print(f"❌ Test failed: {e}")
            import traceback
            traceback.print_exc()
            return False


def print_sample_data_summary():
    """Print a summary of the sample data structure"""
    print("\n📋 Sample PKL Session Data Structure:")
    print("=" * 40)
    
    session = SAMPLE_PKL_SESSIONS[0]
    print(f"Session ID: {session['id']}")
    print(f"Intent: {session['intent']}")
    print(f"Outcome: {session['outcome']}")
    print(f"File: {session['currentFile']}")
    print(f"Conversation Events: {len(session['conversationEvents'])}")
    print(f"File Changes: {len(session['fileChanges'])}")
    print(f"Code Deltas: {len(session['codeDeltas'])}")
    
    print(f"\nConversation Flow:")
    for i, event in enumerate(session['conversationEvents'][:2]):
        print(f"  {i+1}. {event['role']}: {event['content'][:80]}...")


if __name__ == "__main__":
    print_sample_data_summary()
    
    # Run the test
    success = asyncio.run(test_kura_integration())
    
    if success:
        print("\n🎉 PKL-Kura integration test completed successfully!")
        print("🚀 Ready to integrate with the dashboard!")
    else:
        print("\n❌ Integration test failed. Check the error messages above.")
