#!/bin/bash
# Dataset State Historian - Startup Script

echo "🚀 Starting Dataset State Historian..."
echo "📊 Dashboard: http://127.0.0.1:8081"
echo "📈 Timeline: http://127.0.0.1:8081/timeline"
echo "🔧 API: http://127.0.0.1:8081/api"
echo ""

# Activate virtual environment and run
source venv/bin/activate
python run.py
