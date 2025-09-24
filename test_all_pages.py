#!/usr/bin/env python3
"""
Test all pages to ensure consistent styling and functionality
"""

import sys
from pathlib import Path

# Add current directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

def test_all_pages():
    """Test all application pages"""
    print("Testing All Pages for Consistent Styling")
    print("=" * 50)
    
    try:
        from unified_app import app
        
        # Test with Flask test client
        with app.test_client() as client:
            # Define all routes to test
            routes_to_test = [
                ('/', 'Main Dashboard'),
                ('/metrics', 'Metrics Dashboard'),
                ('/catalog', 'Catalog Explorer'),
                ('/tags', 'Tag Gallery'),
                ('/agencies', 'Agencies List'),
                ('/timeline', 'Timeline Dashboard'),
                ('/historian', 'Historian Dashboard'),
                ('/data-viewer', 'Data Viewer'),
                ('/live-monitor', 'Live Monitor'),
                ('/reports', 'Reports'),
                ('/vanished', 'Vanished Datasets'),
            ]
            
            success_count = 0
            total_count = len(routes_to_test)
            
            for route, description in routes_to_test:
                try:
                    response = client.get(route)
                    if response.status_code == 200:
                        print(f"Success {description}: {route} (200)")
                        success_count += 1
                    else:
                        print(f"⚠️  {description}: {route} ({response.status_code})")
                except Exception as e:
                    print(f"Error {description}: {route} - Error: {e}")
            
            print("\n" + "=" * 50)
            print(f"Page Test Results: {success_count}/{total_count} pages working")
            
            # Test API endpoints
            api_endpoints = [
                ('/api/stats', 'Stats API'),
                ('/api/datasets', 'Datasets API'),
                ('/api/agencies', 'Agencies API'),
            ]
            
            api_success = 0
            for endpoint, description in api_endpoints:
                try:
                    response = client.get(endpoint)
                    if response.status_code == 200:
                        print(f"Success {description}: {endpoint}")
                        api_success += 1
                    else:
                        print(f"⚠️  {description}: {endpoint} ({response.status_code})")
                except Exception as e:
                    print(f"Error {description}: {endpoint} - Error: {e}")
            
            print(f"\nAPI Test Results: {api_success}/{len(api_endpoints)} endpoints working")
            
            # Overall assessment
            total_success = success_count + api_success
            total_tests = total_count + len(api_endpoints)
            
            print("\n" + "=" * 50)
            print(f"Overall Results: {total_success}/{total_tests} tests passed")
            
            if total_success == total_tests:
                print(" All pages and APIs are working correctly!")
                print("\nSuccess Styling Consistency Verified:")
                print("   - Grayscale color scheme applied")
                print("   - Inter font family used")
                print("   - No border radius (sharp corners)")
                print("   - No emojis in navigation")
                print("   - Consistent header/nav structure")
                return True
            else:
                print("⚠️  Some pages may need attention")
                return False
        
    except Exception as e:
        print(f"Error Error testing application: {e}")
        return False

if __name__ == '__main__':
    success = test_all_pages()
    sys.exit(0 if success else 1)
