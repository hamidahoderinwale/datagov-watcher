#!/usr/bin/env python3
"""
Simple Dataset Monitor Startup Script
Handles import paths correctly and starts the application
"""

import sys
import os
from pathlib import Path

# Add the src directory to Python path
project_root = Path(__file__).parent
src_path = project_root / "src"
sys.path.insert(0, str(src_path))

def main():
    """Start the Dataset State Historian"""
    print("🚀 Starting Dataset State Historian...")
    print("=" * 50)
    
    try:
        # Change to src directory for relative imports to work
        os.chdir(src_path)
        
        # Create a symlink to the database in the src directory
        db_path = project_root / 'datasets.db'
        src_db_path = src_path / 'datasets.db'
        
        if db_path.exists() and not src_db_path.exists():
            src_db_path.symlink_to(db_path)
        
        # Import the Flask app from the unified_app module
        from unified_app import app, socketio
        
        print("✅ Application loaded successfully")
        print(f"📂 Working directory: {os.getcwd()}")
        print(f"🌐 Dashboard: http://127.0.0.1:8081")
        print(f"📊 API: http://127.0.0.1:8081/api")
        print("=" * 50)
        
        # Start the application
        socketio.run(
            app, 
            host='127.0.0.1', 
            port=8081, 
            debug=True,
            allow_unsafe_werkzeug=True
        )
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("Make sure all dependencies are installed")
        return 1
    except Exception as e:
        print(f"❌ Error starting application: {e}")
        return 1

if __name__ == '__main__':
    sys.exit(main())
