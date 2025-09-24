#!/usr/bin/env python3
"""
Dataset State Historian - Simple Launcher
"""

import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.unified_app import app, socketio

if __name__ == "__main__":
    print("🚀 Starting Dataset State Historian...")
    print("📊 Dashboard: http://127.0.0.1:8081")
    print("📈 Timeline: http://127.0.0.1:8081/timeline")
    print("🔧 API: http://127.0.0.1:8081/api")
    print("")
    
    # Run the application
    socketio.run(
        app, 
        host='127.0.0.1', 
        port=8081, 
        debug=True,
        allow_unsafe_werkzeug=True
    )
