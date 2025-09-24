#!/bin/bash
# PKL-Dash Startup Script
# Comprehensive startup for the enhanced PKL Extension with OpenClio & Kura integration

echo "🚀 Starting PKL-Dash - Enhanced Procedural Knowledge Library"
echo "============================================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if we're in the right directory
if [ ! -f "cursor-pkl-extension/package.json" ]; then
    echo -e "${RED}❌ Error: Not in the correct directory. Please run from the project root.${NC}"
    exit 1
fi

echo -e "${BLUE}📁 Current directory: $(pwd)${NC}"
echo -e "${BLUE}🔧 Setting up environment...${NC}"

# Navigate to the main project directory
cd cursor-pkl-extension

# Check Node.js dependencies
echo -e "${YELLOW}📦 Checking Node.js dependencies...${NC}"
if [ ! -d "node_modules" ]; then
    echo -e "${YELLOW}Installing Node.js dependencies...${NC}"
    npm install
fi

# Check Python virtual environment
echo -e "${YELLOW}🐍 Checking Python environment...${NC}"
if [ ! -d "kura_env" ]; then
    echo -e "${YELLOW}Creating Python virtual environment...${NC}"
    python3 -m venv kura_env
    source kura_env/bin/activate
    pip install --upgrade pip
    pip install kura numpy pandas asyncio rich pathlib dataclasses
    echo -e "${GREEN}✅ Python environment created${NC}"
else
    source kura_env/bin/activate
    echo -e "${GREEN}✅ Python environment activated${NC}"
fi

# Generate sample data if needed
echo -e "${YELLOW}📊 Preparing sample data...${NC}"
if [ ! -f "temp_sessions.json" ]; then
    echo -e "${YELLOW}Creating sample PKL sessions...${NC}"
    cat > temp_sessions.json << 'EOF'
[
  {
    "id": "session_001",
    "timestamp": "2024-01-15T10:30:00Z",
    "intent": "explore",
    "outcome": "success",
    "confidence": 0.92,
    "currentFile": "customer_analysis.ipynb",
    "phase": "analysis",
    "privacyMode": false,
    "codeDeltas": [
      {
        "afterContent": "import pandas as pd\nimport numpy as np\nimport matplotlib.pyplot as plt\n\n# Load customer data\ndf = pd.read_csv('customer_data.csv')\nprint(f'Dataset shape: {df.shape}')\nprint(df.head())"
      },
      {
        "afterContent": "# Data quality check\nprint('Missing values:')\nprint(df.isnull().sum())\n\n# Basic statistics\nprint('\\nBasic statistics:')\nprint(df.describe())"
      }
    ],
    "conversationEvents": [
      {
        "role": "user",
        "content": "I need to analyze customer data to understand purchasing patterns",
        "timestamp": "2024-01-15T10:30:00Z"
      },
      {
        "role": "assistant",
        "content": "I'll help you explore the customer dataset. Let's start by loading the data and checking its structure.",
        "timestamp": "2024-01-15T10:31:00Z"
      },
      {
        "role": "user",
        "content": "Great! Can you also help me check data quality?",
        "timestamp": "2024-01-15T10:35:00Z"
      },
      {
        "role": "assistant",
        "content": "Absolutely! I'll add code to check for missing values and generate basic statistics.",
        "timestamp": "2024-01-15T10:36:00Z"
      }
    ]
  },
  {
    "id": "session_002",
    "timestamp": "2024-01-15T14:20:00Z",
    "intent": "debug",
    "outcome": "stuck",
    "confidence": 0.65,
    "currentFile": "model_training.py",
    "phase": "debugging",
    "privacyMode": false,
    "codeDeltas": [
      {
        "afterContent": "from sklearn.ensemble import RandomForestClassifier\nfrom sklearn.model_selection import train_test_split\nfrom sklearn.metrics import accuracy_score\n\n# Load preprocessed data\nX = pd.read_csv('features.csv')\ny = pd.read_csv('target.csv')\n\n# Split data\nX_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)"
      },
      {
        "afterContent": "# Train model\nmodel = RandomForestClassifier(n_estimators=100, random_state=42)\nmodel.fit(X_train, y_train)\n\n# Make predictions\ny_pred = model.predict(X_test)\naccuracy = accuracy_score(y_test, y_pred)\nprint(f'Accuracy: {accuracy}')\n\n# The accuracy is only 0.52, something seems wrong"
      }
    ],
    "conversationEvents": [
      {
        "role": "user",
        "content": "My RandomForest model is only getting 52% accuracy. Can you help me debug this?",
        "timestamp": "2024-01-15T14:20:00Z"
      },
      {
        "role": "assistant",
        "content": "That's quite low for a RandomForest. Let's check the data preprocessing and feature engineering steps.",
        "timestamp": "2024-01-15T14:21:00Z"
      }
    ]
  },
  {
    "id": "session_003",
    "timestamp": "2024-01-15T16:45:00Z",
    "intent": "implement",
    "outcome": "success",
    "confidence": 0.88,
    "currentFile": "visualization_dashboard.py",
    "phase": "implementation",
    "privacyMode": false,
    "codeDeltas": [
      {
        "afterContent": "import plotly.express as px\nimport plotly.graph_objects as go\nfrom plotly.subplots import make_subplots\nimport streamlit as st\n\n# Dashboard title\nst.title('Customer Analytics Dashboard')\nst.sidebar.header('Filters')"
      },
      {
        "afterContent": "# Load data\n@st.cache_data\ndef load_data():\n    return pd.read_csv('customer_data.csv')\n\ndf = load_data()\n\n# Create interactive charts\nfig1 = px.histogram(df, x='age', title='Customer Age Distribution')\nst.plotly_chart(fig1)\n\nfig2 = px.scatter(df, x='income', y='spending_score', title='Income vs Spending Score')\nst.plotly_chart(fig2)"
      }
    ],
    "conversationEvents": [
      {
        "role": "user",
        "content": "I want to create an interactive dashboard to visualize customer analytics",
        "timestamp": "2024-01-15T16:45:00Z"
      },
      {
        "role": "assistant",
        "content": "Perfect! I'll help you build a Streamlit dashboard with Plotly visualizations for your customer data.",
        "timestamp": "2024-01-15T16:46:00Z"
      }
    ]
  }
]
EOF
    echo -e "${GREEN}✅ Sample data created${NC}"
fi

# Run repository parsing and native integration
echo -e "${BLUE}🔬 Running repository parsing and native integration...${NC}"
python repository_parser.py
python native_pkl_integration.py --sessions-file temp_sessions.json

# Start the API server in background
echo -e "${BLUE}🌐 Starting API server...${NC}"
if ! pgrep -f "kura-api-endpoint.js" > /dev/null; then
    nohup node kura-api-endpoint.js > api-server.log 2>&1 &
    API_PID=$!
    echo -e "${GREEN}✅ API server started (PID: $API_PID)${NC}"
    sleep 2
else
    echo -e "${GREEN}✅ API server already running${NC}"
fi

# Start the web server for the dashboard
echo -e "${BLUE}🎨 Starting enhanced dashboard...${NC}"
if ! pgrep -f "web-server.js" > /dev/null; then
    nohup node web-server.js > web-server.log 2>&1 &
    WEB_PID=$!
    echo -e "${GREEN}✅ Web server started (PID: $WEB_PID)${NC}"
    sleep 2
else
    echo -e "${GREEN}✅ Web server already running${NC}"
fi

echo ""
echo -e "${GREEN}🎉 PKL-Dash is now running!${NC}"
echo "=============================================="
echo ""
echo -e "${BLUE}📊 Available Interfaces:${NC}"
echo -e "  🌐 Main Dashboard:      ${YELLOW}http://localhost:8080${NC}"
echo -e "  🔬 Enhanced Dashboard:  ${YELLOW}http://localhost:8080/kura-enhanced-dashboard.html${NC}"
echo -e "  📡 API Endpoint:        ${YELLOW}http://localhost:3001${NC}"
echo -e "  📈 Live Monitor:        ${YELLOW}http://localhost:8080/live-dashboard-clean.html${NC}"
echo ""
echo -e "${BLUE}📁 Generated Analysis Files:${NC}"
echo -e "  📊 Repository Analysis:    ${YELLOW}repository_analysis.json${NC}"
echo -e "  🔧 Integration Spec:       ${YELLOW}pkl_integration_spec.json${NC}"
echo -e "  🧬 Native PKL Analysis:    ${YELLOW}native_pkl_output/native_pkl_analysis.json${NC}"
echo -e "  🎨 Dashboard Config:       ${YELLOW}native_pkl_output/enhanced_dashboard_config.json${NC}"
echo ""
echo -e "${BLUE}🔧 Key Features Available:${NC}"
echo -e "  ✅ OpenClio faceted analysis (6 PKL-specific facets)"
echo -e "  ✅ Kura hierarchical clustering with UMAP visualization"
echo -e "  ✅ Native PKL conversation processing"
echo -e "  ✅ Automated procedure template generation"
echo -e "  ✅ Real-time session monitoring"
echo -e "  ✅ Interactive cluster exploration"
echo ""
echo -e "${YELLOW}💡 Quick Start:${NC}"
echo -e "  1. Open ${YELLOW}http://localhost:8080/kura-enhanced-dashboard.html${NC}"
echo -e "  2. Explore the hierarchical clusters in the left panel"
echo -e "  3. Click on UMAP points to see session details"
echo -e "  4. Check generated procedure templates"
echo ""
echo -e "${YELLOW}🛑 To stop all services:${NC}"
echo -e "  Run: ${YELLOW}pkill -f 'kura-api-endpoint\\|web-server'${NC}"
echo ""
echo -e "${GREEN}Ready for data science procedural knowledge capture! 🚀${NC}"
