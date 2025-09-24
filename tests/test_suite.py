"""
Comprehensive Test Suite for Dataset State Historian
"""

import unittest
import requests
import json
import time
import os
import sqlite3
from datetime import datetime, timedelta

class TestDatasetStateHistorian(unittest.TestCase):
    def setUp(self):
        """Set up test environment"""
        self.base_url = "http://127.0.0.1:8089"
        self.api_base = f"{self.base_url}/api"
        self.session = requests.Session()
        
        # Test credentials
        self.test_user = {
            'username': 'testuser',
            'email': 'test@example.com',
            'password': 'testpassword123'
        }
        
        self.auth_token = None
    
    def tearDown(self):
        """Clean up after tests"""
        if self.auth_token:
            # Logout if authenticated
            try:
                self.session.post(f"{self.api_base}/auth/logout")
            except:
                pass
    
    def test_01_health_check(self):
        """Test basic health check"""
        response = self.session.get(f"{self.api_base}/health/overview")
        self.assertEqual(response.status_code, 200)
        
        data = response.json()
        self.assertIn('system_status', data)
        self.assertIn('response_time', data)
        self.assertIn('memory_usage', data)
    
    def test_02_user_registration(self):
        """Test user registration"""
        response = self.session.post(f"{self.api_base}/auth/register", json=self.test_user)
        self.assertIn(response.status_code, [200, 201, 400])  # 400 if user already exists
        
        if response.status_code == 201:
            data = response.json()
            self.assertIn('message', data)
    
    def test_03_user_login(self):
        """Test user login"""
        response = self.session.post(f"{self.api_base}/auth/login", json={
            'username': self.test_user['username'],
            'password': self.test_user['password']
        })
        
        if response.status_code == 200:
            data = response.json()
            self.assertIn('user', data)
            self.assertIn('session_token', data)
            self.auth_token = data['session_token']
            
            # Set authorization header for future requests
            self.session.headers.update({'Authorization': f'Bearer {self.auth_token}'})
        else:
            # Try with admin credentials
            admin_response = self.session.post(f"{self.api_base}/auth/login", json={
                'username': 'admin',
                'password': 'admin123'
            })
            self.assertEqual(admin_response.status_code, 200)
            admin_data = admin_response.json()
            self.auth_token = admin_data['session_token']
            self.session.headers.update({'Authorization': f'Bearer {self.auth_token}'})
    
    def test_04_api_endpoints(self):
        """Test main API endpoints"""
        endpoints = [
            '/stats',
            '/datasets',
            '/agencies',
            '/wayback/stats',
            '/search/',
            '/quality/summary',
            '/health/overview',
            '/backup/list'
        ]
        
        for endpoint in endpoints:
            with self.subTest(endpoint=endpoint):
                response = self.session.get(f"{self.api_base}{endpoint}")
                self.assertIn(response.status_code, [200, 401, 403])  # 401/403 if auth required
    
    def test_05_dataset_operations(self):
        """Test dataset operations"""
        # Get datasets list
        response = self.session.get(f"{self.api_base}/datasets?limit=5")
        self.assertIn(response.status_code, [200, 401, 403])
        
        if response.status_code == 200:
            data = response.json()
            self.assertIn('datasets', data)
            self.assertIn('total', data)
            
            # Test individual dataset if available
            if data['datasets']:
                dataset_id = data['datasets'][0]['id']
                
                # Test dataset metadata
                meta_response = self.session.get(f"{self.api_base}/metadata/dataset/{dataset_id}")
                self.assertIn(meta_response.status_code, [200, 401, 403])
                
                # Test wayback timeline
                timeline_response = self.session.get(f"{self.api_base}/wayback/timeline/{dataset_id}")
                self.assertIn(timeline_response.status_code, [200, 401, 403])
    
    def test_06_search_functionality(self):
        """Test search functionality"""
        search_queries = ['census', 'health', 'education', 'transportation']
        
        for query in search_queries:
            with self.subTest(query=query):
                response = self.session.get(f"{self.api_base}/search/?q={query}&limit=5")
                self.assertIn(response.status_code, [200, 401, 403])
                
                if response.status_code == 200:
                    data = response.json()
                    self.assertIn('results', data)
                    self.assertIn('total_results', data)
    
    def test_07_analytics_endpoints(self):
        """Test analytics endpoints"""
        analytics_endpoints = [
            '/analytics/trends?period=30',
            '/analytics/status',
            '/analytics/agencies',
            '/analytics/changes?period=7',
            '/analytics/quality'
        ]
        
        for endpoint in analytics_endpoints:
            with self.subTest(endpoint=endpoint):
                response = self.session.get(f"{self.api_base}{endpoint}")
                self.assertIn(response.status_code, [200, 401, 403])
    
    def test_08_export_functionality(self):
        """Test export functionality"""
        export_endpoints = [
            '/export/analytics?format=json&time_period=7',
            '/export/agencies?format=csv',
            '/export/dataset/072148e9-e16f-4213-ae59-628cbebc11fa?format=json'
        ]
        
        for endpoint in export_endpoints:
            with self.subTest(endpoint=endpoint):
                response = self.session.get(f"{self.api_base}{endpoint}")
                self.assertIn(response.status_code, [200, 401, 403, 404])
    
    def test_09_data_quality(self):
        """Test data quality assessment"""
        response = self.session.get(f"{self.api_base}/quality/summary")
        self.assertIn(response.status_code, [200, 401, 403])
        
        if response.status_code == 200:
            data = response.json()
            self.assertIn('total_datasets', data)
            self.assertIn('available_datasets', data)
            self.assertIn('overall_quality_score', data)
    
    def test_10_system_health(self):
        """Test system health monitoring"""
        health_endpoints = [
            '/health/overview',
            '/health/detailed',
            '/health/logs',
            '/health/alerts',
            '/health/performance'
        ]
        
        for endpoint in health_endpoints:
            with self.subTest(endpoint=endpoint):
                response = self.session.get(f"{self.api_base}{endpoint}")
                self.assertIn(response.status_code, [200, 401, 403])
    
    def test_11_backup_operations(self):
        """Test backup operations"""
        if not self.auth_token:
            self.skipTest("Authentication required for backup operations")
        
        # Test backup creation
        response = self.session.post(f"{self.api_base}/backup/create", json={
            'type': 'database_only',
            'compress': True
        })
        self.assertIn(response.status_code, [200, 201, 401, 403])
        
        # Test backup listing
        list_response = self.session.get(f"{self.api_base}/backup/list")
        self.assertIn(list_response.status_code, [200, 401, 403])
        
        if list_response.status_code == 200:
            data = list_response.json()
            self.assertIn('backups', data)
    
    def test_12_rate_limiting(self):
        """Test rate limiting"""
        # Make multiple rapid requests to test rate limiting
        responses = []
        for i in range(10):
            response = self.session.get(f"{self.api_base}/stats")
            responses.append(response.status_code)
            time.sleep(0.1)
        
        # Should not all be 429 (rate limited)
        rate_limited_count = responses.count(429)
        self.assertLess(rate_limited_count, 10)
    
    def test_13_web_pages(self):
        """Test web page accessibility"""
        pages = [
            '/',
            '/search',
            '/wayback',
            '/analytics',
            '/visualization',
            '/data-quality',
            '/system-health',
            '/api-docs'
        ]
        
        for page in pages:
            with self.subTest(page=page):
                response = self.session.get(f"{self.base_url}{page}")
                self.assertEqual(response.status_code, 200)
                self.assertIn('text/html', response.headers.get('content-type', ''))
    
    def test_14_database_integrity(self):
        """Test database integrity"""
        if not os.path.exists('datasets.db'):
            self.skipTest("Database file not found")
        
        conn = sqlite3.connect('datasets.db')
        cursor = conn.cursor()
        
        try:
            # Test main tables exist
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            
            expected_tables = [
                'datasets', 'dataset_states', 'state_diffs', 'users', 
                'user_sessions', 'backup_history', 'rate_limits'
            ]
            
            for table in expected_tables:
                self.assertIn(table, tables, f"Table {table} not found")
            
            # Test data integrity
            cursor.execute("SELECT COUNT(*) FROM datasets")
            dataset_count = cursor.fetchone()[0]
            self.assertGreater(dataset_count, 0, "No datasets found")
            
            cursor.execute("SELECT COUNT(*) FROM dataset_states")
            state_count = cursor.fetchone()[0]
            self.assertGreater(state_count, 0, "No dataset states found")
            
        finally:
            conn.close()
    
    def test_15_performance_metrics(self):
        """Test performance metrics"""
        start_time = time.time()
        
        # Test multiple concurrent requests
        import threading
        results = []
        
        def make_request():
            response = self.session.get(f"{self.api_base}/stats")
            results.append(response.status_code)
        
        threads = []
        for i in range(5):
            thread = threading.Thread(target=make_request)
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()
        
        end_time = time.time()
        response_time = end_time - start_time
        
        # Should complete within reasonable time
        self.assertLess(response_time, 10, "Performance test took too long")
        
        # Most requests should succeed
        success_count = sum(1 for status in results if status == 200)
        self.assertGreater(success_count, 0, "No successful requests")
    
    def test_16_error_handling(self):
        """Test error handling"""
        # Test invalid endpoints
        response = self.session.get(f"{self.api_base}/invalid/endpoint")
        self.assertEqual(response.status_code, 404)
        
        # Test invalid dataset ID
        response = self.session.get(f"{self.api_base}/metadata/dataset/invalid-id")
        self.assertIn(response.status_code, [400, 404, 401, 403])
        
        # Test malformed JSON
        response = self.session.post(f"{self.api_base}/auth/login", 
                                   data="invalid json",
                                   headers={'Content-Type': 'application/json'})
        self.assertIn(response.status_code, [400, 415])
    
    def test_17_security_headers(self):
        """Test security headers"""
        response = self.session.get(f"{self.base_url}/")
        
        security_headers = [
            'X-Content-Type-Options',
            'X-Frame-Options',
            'X-XSS-Protection'
        ]
        
        for header in security_headers:
            self.assertIn(header, response.headers, f"Security header {header} missing")
    
    def test_18_cors_headers(self):
        """Test CORS headers"""
        response = self.session.options(f"{self.api_base}/stats")
        
        # Should handle OPTIONS request
        self.assertIn(response.status_code, [200, 204, 405])
    
    def test_19_data_consistency(self):
        """Test data consistency"""
        if not os.path.exists('datasets.db'):
            self.skipTest("Database file not found")
        
        conn = sqlite3.connect('datasets.db')
        cursor = conn.cursor()
        
        try:
            # Test that dataset states have valid dataset references
            cursor.execute("""
                SELECT COUNT(*) FROM dataset_states ds
                LEFT JOIN datasets d ON ds.dataset_id = d.id
                WHERE d.id IS NULL
            """)
            orphaned_states = cursor.fetchone()[0]
            self.assertEqual(orphaned_states, 0, "Found orphaned dataset states")
            
            # Test that state diffs have valid dataset references
            cursor.execute("""
                SELECT COUNT(*) FROM state_diffs sd
                LEFT JOIN datasets d ON sd.dataset_id = d.id
                WHERE d.id IS NULL
            """)
            orphaned_diffs = cursor.fetchone()[0]
            self.assertEqual(orphaned_diffs, 0, "Found orphaned state diffs")
            
        finally:
            conn.close()

def run_tests():
    """Run the test suite"""
    print("🧪 Starting Comprehensive Test Suite for Dataset State Historian")
    print("=" * 70)
    
    # Create test suite
    suite = unittest.TestLoader().loadTestsFromTestCase(TestDatasetStateHistorian)
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    print("\n" + "=" * 70)
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Success rate: {((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100):.1f}%")
    
    if result.failures:
        print("\nFailures:")
        for test, traceback in result.failures:
            print(f"  - {test}: {traceback.split('AssertionError: ')[-1].split('\\n')[0]}")
    
    if result.errors:
        print("\nErrors:")
        for test, traceback in result.errors:
            print(f"  - {test}: {traceback.split('\\n')[-2]}")
    
    return result.wasSuccessful()

if __name__ == '__main__':
    success = run_tests()
    exit(0 if success else 1)

