#!/usr/bin/env python3
"""
Simple Dataset Monitor Startup Script
Properly handles import paths and starts the application
"""

import sys
import os
from pathlib import Path

def main():
    """Start the Dataset State Historian with proper path setup"""
    print("Starting Starting Dataset State Historian...")
    print("=" * 50)
    
    # Get the project root directory
    project_root = Path(__file__).parent.absolute()
    src_path = project_root / "src"
    
    # Add src to Python path
    sys.path.insert(0, str(src_path))
    
    # Change working directory to src for relative imports
    original_cwd = os.getcwd()
    os.chdir(src_path)
    
    try:
        # Import the Flask app
        from unified_app import app, socketio
        
        print("Success Application loaded successfully")
        print(f" Working directory: {os.getcwd()}")
        print(f" Dashboard: http://127.0.0.1:8081")
        print(f" API: http://127.0.0.1:8081/api")
        print("=" * 50)
        
        # Start the application with debug disabled to avoid restart issues
        socketio.run(
            app, 
            host='127.0.0.1', 
            port=8081, 
            debug=False,
            allow_unsafe_werkzeug=True
        )
        
    except ImportError as e:
        print(f"Error Import error: {e}")
        print("Make sure all dependencies are installed:")
        print("  pip install -r requirements.txt")
        return 1
    except Exception as e:
        print(f"Error Error starting application: {e}")
        return 1
    finally:
        # Restore original working directory
        os.chdir(original_cwd)

if __name__ == '__main__':
    sys.exit(main())
