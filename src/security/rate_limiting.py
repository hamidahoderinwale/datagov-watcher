"""
API Rate Limiting and Security System
"""

import time
import sqlite3
from datetime import datetime, timedelta
from functools import wraps
from flask import request, jsonify, g
import hashlib
import ipaddress

class RateLimiter:
    def __init__(self, db_path='datasets.db'):
        self.db_path = db_path
        self.init_rate_limit_tables()
    
    def init_rate_limit_tables(self):
        """Initialize rate limiting database tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Rate limit tracking table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS rate_limits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                identifier VARCHAR(255) NOT NULL,
                endpoint VARCHAR(100) NOT NULL,
                request_count INTEGER DEFAULT 1,
                window_start TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_request TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                blocked_until TIMESTAMP NULL,
                UNIQUE(identifier, endpoint)
            )
        ''')
        
        # Security events table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS security_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type VARCHAR(50) NOT NULL,
                identifier VARCHAR(255) NOT NULL,
                ip_address VARCHAR(45) NOT NULL,
                user_agent TEXT,
                endpoint VARCHAR(100),
                details TEXT,
                severity VARCHAR(20) DEFAULT 'medium',
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # IP whitelist/blacklist table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ip_access_control (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ip_address VARCHAR(45) NOT NULL,
                ip_range VARCHAR(45),
                action VARCHAR(10) NOT NULL, -- 'allow' or 'block'
                reason TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP NULL,
                is_active BOOLEAN DEFAULT 1
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def get_client_identifier(self, request):
        """Get unique client identifier for rate limiting"""
        # Try to get user ID from session if authenticated
        if hasattr(request, 'current_user') and request.current_user:
            return f"user:{request.current_user['id']}"
        
        # Fall back to IP address
        ip = self.get_client_ip(request)
        return f"ip:{ip}"
    
    def get_client_ip(self, request):
        """Get client IP address"""
        # Check for forwarded headers first
        if request.headers.get('X-Forwarded-For'):
            return request.headers.get('X-Forwarded-For').split(',')[0].strip()
        elif request.headers.get('X-Real-IP'):
            return request.headers.get('X-Real-IP')
        else:
            return request.remote_addr
    
    def is_ip_allowed(self, ip_address):
        """Check if IP address is allowed"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Check for explicit blocks first
            cursor.execute('''
                SELECT action FROM ip_access_control 
                WHERE (ip_address = ? OR ? LIKE ip_range || '%')
                AND action = 'block' 
                AND is_active = 1 
                AND (expires_at IS NULL OR expires_at > ?)
                ORDER BY 
                    CASE WHEN ip_address = ? THEN 1 ELSE 2 END,
                    created_at DESC
                LIMIT 1
            ''', (ip_address, ip_address, datetime.now().isoformat(), ip_address))
            
            block_result = cursor.fetchone()
            if block_result:
                return False
            
            # Check for explicit allows
            cursor.execute('''
                SELECT action FROM ip_access_control 
                WHERE (ip_address = ? OR ? LIKE ip_range || '%')
                AND action = 'allow' 
                AND is_active = 1 
                AND (expires_at IS NULL OR expires_at > ?)
                ORDER BY 
                    CASE WHEN ip_address = ? THEN 1 ELSE 2 END,
                    created_at DESC
                LIMIT 1
            ''', (ip_address, ip_address, datetime.now().isoformat(), ip_address))
            
            allow_result = cursor.fetchone()
            return allow_result is not None
            
        except Exception as e:
            print(f"Error checking IP access: {e}")
            return True  # Default to allowed on error
        finally:
            conn.close()
    
    def check_rate_limit(self, identifier, endpoint, limit, window_seconds):
        """Check if request is within rate limit"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            current_time = datetime.now()
            window_start = current_time - timedelta(seconds=window_seconds)
            
            # Get current rate limit data
            cursor.execute('''
                SELECT request_count, window_start, blocked_until
                FROM rate_limits 
                WHERE identifier = ? AND endpoint = ?
            ''', (identifier, endpoint))
            
            result = cursor.fetchone()
            
            if result:
                request_count, window_start_time, blocked_until = result
                window_start_time = datetime.fromisoformat(window_start_time)
                
                # Check if currently blocked
                if blocked_until and current_time < datetime.fromisoformat(blocked_until):
                    return False, f"Rate limit exceeded. Blocked until {blocked_until}"
                
                # Check if window has expired
                if window_start_time < window_start:
                    # Reset window
                    cursor.execute('''
                        UPDATE rate_limits 
                        SET request_count = 1, window_start = ?, last_request = ?
                        WHERE identifier = ? AND endpoint = ?
                    ''', (current_time.isoformat(), current_time.isoformat(), identifier, endpoint))
                    conn.commit()
                    return True, "Rate limit OK"
                
                # Check if limit exceeded
                if request_count >= limit:
                    # Block for increasing duration
                    block_duration = min(300, (request_count - limit + 1) * 60)  # Max 5 minutes
                    blocked_until = current_time + timedelta(seconds=block_duration)
                    
                    cursor.execute('''
                        UPDATE rate_limits 
                        SET blocked_until = ?, last_request = ?
                        WHERE identifier = ? AND endpoint = ?
                    ''', (blocked_until.isoformat(), current_time.isoformat(), identifier, endpoint))
                    conn.commit()
                    
                    return False, f"Rate limit exceeded. Blocked for {block_duration} seconds"
                
                # Increment request count
                cursor.execute('''
                    UPDATE rate_limits 
                    SET request_count = request_count + 1, last_request = ?
                    WHERE identifier = ? AND endpoint = ?
                ''', (current_time.isoformat(), identifier, endpoint))
                conn.commit()
                
                return True, "Rate limit OK"
            
            else:
                # First request for this identifier/endpoint
                cursor.execute('''
                    INSERT INTO rate_limits (identifier, endpoint, request_count, window_start, last_request)
                    VALUES (?, ?, 1, ?, ?)
                ''', (identifier, endpoint, current_time.isoformat(), current_time.isoformat()))
                conn.commit()
                
                return True, "Rate limit OK"
                
        except Exception as e:
            print(f"Error checking rate limit: {e}")
            return True, "Rate limit check failed"  # Default to allowed on error
        finally:
            conn.close()
    
    def log_security_event(self, event_type, identifier, ip_address, endpoint, details, severity='medium'):
        """Log security event"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT INTO security_events (event_type, identifier, ip_address, user_agent, endpoint, details, severity)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (event_type, identifier, ip_address, request.headers.get('User-Agent'), endpoint, details, severity))
            conn.commit()
        except Exception as e:
            print(f"Error logging security event: {e}")
        finally:
            conn.close()
    
    def get_rate_limit_status(self, identifier):
        """Get current rate limit status for identifier"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT endpoint, request_count, window_start, blocked_until
                FROM rate_limits 
                WHERE identifier = ?
                ORDER BY last_request DESC
            ''', (identifier,))
            
            results = cursor.fetchall()
            status = {}
            
            for row in results:
                endpoint, count, window_start, blocked_until = row
                status[endpoint] = {
                    'request_count': count,
                    'window_start': window_start,
                    'blocked_until': blocked_until,
                    'is_blocked': blocked_until and datetime.now() < datetime.fromisoformat(blocked_until)
                }
            
            return status
        except Exception as e:
            print(f"Error getting rate limit status: {e}")
            return {}
        finally:
            conn.close()

# Global rate limiter instance
rate_limiter = RateLimiter()

def rate_limit(limit=100, window_seconds=3600, per_user=True):
    """Decorator for rate limiting API endpoints"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            # Get client identifier
            identifier = rate_limiter.get_client_identifier(request)
            endpoint = f"{request.method}:{request.endpoint}"
            
            # Check IP access
            ip_address = rate_limiter.get_client_ip(request)
            if not rate_limiter.is_ip_allowed(ip_address):
                rate_limiter.log_security_event(
                    'ip_blocked', identifier, ip_address, endpoint,
                    f"IP {ip_address} is blocked", 'high'
                )
                return jsonify({'error': 'Access denied'}), 403
            
            # Check rate limit
            allowed, message = rate_limiter.check_rate_limit(identifier, endpoint, limit, window_seconds)
            
            if not allowed:
                rate_limiter.log_security_event(
                    'rate_limit_exceeded', identifier, ip_address, endpoint,
                    f"Rate limit exceeded: {message}", 'medium'
                )
                return jsonify({
                    'error': 'Rate limit exceeded',
                    'message': message,
                    'retry_after': 60
                }), 429
            
            # Add rate limit info to response headers
            g.rate_limit_info = {
                'limit': limit,
                'window_seconds': window_seconds,
                'identifier': identifier
            }
            
            return f(*args, **kwargs)
        return decorated_function
    return decorator

def security_headers(f):
    """Decorator to add security headers"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        response = f(*args, **kwargs)
        
        if hasattr(response, 'headers'):
            # Add security headers
            response.headers['X-Content-Type-Options'] = 'nosniff'
            response.headers['X-Frame-Options'] = 'DENY'
            response.headers['X-XSS-Protection'] = '1; mode=block'
            response.headers['Strict-Transport-Security'] = 'max-age=31536000; includeSubDomains'
            response.headers['Referrer-Policy'] = 'strict-origin-when-cross-origin'
            response.headers['Content-Security-Policy'] = "default-src 'self'"
        
        return response
    return decorated_function

def validate_input(f):
    """Decorator to validate input data"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Check for SQL injection patterns
        if request.method in ['POST', 'PUT', 'PATCH']:
            data = request.get_json() or {}
            for key, value in data.items():
                if isinstance(value, str):
                    # Basic SQL injection detection
                    sql_patterns = ['union', 'select', 'insert', 'update', 'delete', 'drop', 'create', 'alter']
                    if any(pattern in value.lower() for pattern in sql_patterns):
                        rate_limiter.log_security_event(
                            'sql_injection_attempt', 
                            rate_limiter.get_client_identifier(request),
                            rate_limiter.get_client_ip(request),
                            f"{request.method}:{request.endpoint}",
                            f"Potential SQL injection in field {key}",
                            'high'
                        )
                        return jsonify({'error': 'Invalid input detected'}), 400
        
        return f(*args, **kwargs)
    return decorated_function

