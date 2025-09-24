#!/usr/bin/env python3
"""
Start the Dataset State Historian Application
Simple startup script with proper configuration
"""

import os
import sys
from pathlib import Path

# Add current directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

def main():
    """Start the application"""
    print("Dataset State Historian - Starting Application")
    print("=" * 60)
    
    try:
        from unified_app import app, socketio
        
        print("Application loaded successfully")
        print(f"Template folder: {app.template_folder}")
        print(f"Static folder: {app.static_folder}")
        print()
        
        # Check if database exists
        db_path = "datasets.db"
        if os.path.exists(db_path):
            print(f"Database found: {db_path}")
        else:
            print(f"Database not found: {db_path}")
            print("   The app will create it on first run")
        
        print()
        print("Starting server...")
        print("Dashboard: http://127.0.0.1:8089")
        print("API: http://127.0.0.1:8089/api/stats")
        print()
        print("Press Ctrl+C to stop the server")
        print("-" * 60)
        
        # Start the application with production warning disabled
        socketio.run(app, debug=True, host='127.0.0.1', port=8089, allow_unsafe_werkzeug=True)
        
    except ImportError as e:
        print(f"Import error: {e}")
        print("Make sure you're in the correct directory and have installed dependencies")
        sys.exit(1)
    except Exception as e:
        print(f"Error starting application: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
