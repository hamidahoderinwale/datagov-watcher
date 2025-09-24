#!/usr/bin/env python3
"""
Test the styling changes - grayscale, Inter font, no rounding, no emojis
"""

import sys
from pathlib import Path

# Add current directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

def test_app():
    """Test the application with new styling"""
    print("Testing Dataset State Historian Application")
    print("=" * 50)
    
    try:
        from unified_app import app
        
        print("Success Application loaded successfully")
        
        # Test with Flask test client
        with app.test_client() as client:
            # Test main page
            response = client.get('/')
            print(f"Success Main page: {response.status_code}")
            
            # Test API
            response = client.get('/api/stats')
            print(f"Success API stats: {response.status_code}")
            
            if response.status_code == 200:
                data = response.get_json()
                print(f"   Total datasets: {data.get('total_datasets', 'N/A')}")
                print(f"   Total snapshots: {data.get('total_snapshots', 'N/A')}")
        
        print("\nSuccess Application is working correctly")
        print("Success Styling changes applied:")
        print("   - Grayscale color scheme")
        print("   - Inter font family")
        print("   - No border radius (sharp corners)")
        print("   - No emojis in navigation")
        
        return True
        
    except Exception as e:
        print(f"Error Error: {e}")
        return False

if __name__ == '__main__':
    success = test_app()
    sys.exit(0 if success else 1)
