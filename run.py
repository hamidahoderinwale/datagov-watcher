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
    import os
    
    # Get port from environment variable (for deployment platforms)
    port = int(os.environ.get('PORT', 8081))
    host = os.environ.get('HOST', '0.0.0.0')
    debug = os.environ.get('FLASK_ENV') != 'production'
    
    print("Starting Dataset State Historian...")
    print(f"Dashboard: http://{host}:{port}")
    print(f"Timeline: http://{host}:{port}/timeline")
    print(f"API: http://{host}:{port}/api")
    print("")
    
    # Run the application
    socketio.run(
        app, 
        host=host, 
        port=port, 
        debug=debug,
        allow_unsafe_werkzeug=True
    )
