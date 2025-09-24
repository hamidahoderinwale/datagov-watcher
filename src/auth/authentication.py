"""
User Authentication and Authorization System
"""

import hashlib
import secrets
import sqlite3
from datetime import datetime, timedelta
from functools import wraps
from flask import request, jsonify, session, current_app
import jwt

class UserManager:
    def __init__(self, db_path='datasets.db'):
        self.db_path = db_path
        self.init_user_tables()
    
    def init_user_tables(self):
        """Initialize user-related database tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Users table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username VARCHAR(50) UNIQUE NOT NULL,
                email VARCHAR(100) UNIQUE NOT NULL,
                password_hash VARCHAR(255) NOT NULL,
                salt VARCHAR(255) NOT NULL,
                role VARCHAR(20) DEFAULT 'user',
                is_active BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_login TIMESTAMP,
                failed_login_attempts INTEGER DEFAULT 0,
                locked_until TIMESTAMP NULL
            )
        ''')
        
        # User sessions table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                session_token VARCHAR(255) UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP NOT NULL,
                ip_address VARCHAR(45),
                user_agent TEXT,
                is_active BOOLEAN DEFAULT 1,
                FOREIGN KEY (user_id) REFERENCES users (id)
            )
        ''')
        
        # User permissions table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_permissions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                permission VARCHAR(50) NOT NULL,
                granted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                granted_by INTEGER,
                FOREIGN KEY (user_id) REFERENCES users (id),
                FOREIGN KEY (granted_by) REFERENCES users (id),
                UNIQUE(user_id, permission)
            )
        ''')
        
        # Audit log table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                action VARCHAR(100) NOT NULL,
                resource VARCHAR(100),
                details TEXT,
                ip_address VARCHAR(45),
                user_agent TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (id)
            )
        ''')
        
        conn.commit()
        conn.close()
        
        # Create default admin user if none exists
        self.create_default_admin()
    
    def create_default_admin(self):
        """Create default admin user if none exists"""
        if not self.get_user_by_username('admin'):
            self.create_user(
                username='admin',
                email='admin@datasetmonitor.com',
                password='admin123',
                role='admin'
            )
            print("Default admin user created: admin/admin123")
    
    def hash_password(self, password, salt=None):
        """Hash password with salt"""
        if salt is None:
            salt = secrets.token_hex(32)
        
        password_hash = hashlib.pbkdf2_hmac(
            'sha256',
            password.encode('utf-8'),
            salt.encode('utf-8'),
            100000
        )
        return password_hash.hex(), salt
    
    def verify_password(self, password, password_hash, salt):
        """Verify password against hash"""
        test_hash, _ = self.hash_password(password, salt)
        return test_hash == password_hash
    
    def create_user(self, username, email, password, role='user', **kwargs):
        """Create a new user"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Check if user already exists
            cursor.execute("SELECT id FROM users WHERE username = ? OR email = ?", (username, email))
            if cursor.fetchone():
                return False, "User already exists"
            
            # Hash password
            password_hash, salt = self.hash_password(password)
            
            # Create user
            cursor.execute('''
                INSERT INTO users (username, email, password_hash, salt, role, is_active)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (username, email, password_hash, salt, role, 1))
            
            user_id = cursor.lastrowid
            
            # Grant default permissions based on role
            self.grant_default_permissions(user_id, role)
            
            conn.commit()
            return True, f"User {username} created successfully"
            
        except Exception as e:
            conn.rollback()
            return False, str(e)
        finally:
            conn.close()
    
    def grant_default_permissions(self, user_id, role):
        """Grant default permissions based on role"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Define role-based permissions
            permissions = {
                'admin': [
                    'read_datasets', 'write_datasets', 'delete_datasets',
                    'read_analytics', 'write_analytics', 'export_data',
                    'manage_users', 'manage_system', 'view_audit_log'
                ],
                'analyst': [
                    'read_datasets', 'read_analytics', 'export_data',
                    'view_audit_log'
                ],
                'user': [
                    'read_datasets', 'read_analytics'
                ]
            }
            
            role_permissions = permissions.get(role, permissions['user'])
            
            for permission in role_permissions:
                cursor.execute('''
                    INSERT OR IGNORE INTO user_permissions (user_id, permission)
                    VALUES (?, ?)
                ''', (user_id, permission))
            
            conn.commit()
            
        except Exception as e:
            print(f"Error granting permissions: {e}")
        finally:
            conn.close()
    
    def authenticate_user(self, username, password, ip_address=None, user_agent=None):
        """Authenticate user with username and password"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Get user
            cursor.execute('''
                SELECT id, username, email, password_hash, salt, role, is_active,
                       failed_login_attempts, locked_until
                FROM users WHERE username = ? OR email = ?
            ''', (username, username))
            
            user = cursor.fetchone()
            if not user:
                return False, "Invalid credentials"
            
            user_id, username, email, password_hash, salt, role, is_active, failed_attempts, locked_until = user
            
            # Check if account is locked
            if locked_until and datetime.now() < datetime.fromisoformat(locked_until):
                return False, "Account is locked due to too many failed login attempts"
            
            # Check if account is active
            if not is_active:
                return False, "Account is deactivated"
            
            # Verify password
            if not self.verify_password(password, password_hash, salt):
                # Increment failed login attempts
                cursor.execute('''
                    UPDATE users 
                    SET failed_login_attempts = failed_login_attempts + 1
                    WHERE id = ?
                ''', (user_id,))
                
                # Lock account after 5 failed attempts
                if failed_attempts + 1 >= 5:
                    lock_until = datetime.now() + timedelta(minutes=30)
                    cursor.execute('''
                        UPDATE users 
                        SET locked_until = ?
                        WHERE id = ?
                    ''', (lock_until.isoformat(), user_id))
                
                conn.commit()
                return False, "Invalid credentials"
            
            # Reset failed login attempts on successful login
            cursor.execute('''
                UPDATE users 
                SET failed_login_attempts = 0, locked_until = NULL, last_login = ?
                WHERE id = ?
            ''', (datetime.now().isoformat(), user_id))
            
            conn.commit()
            
            # Log successful login
            self.log_audit_event(user_id, 'login', 'authentication', 
                               f'User {username} logged in', ip_address, user_agent)
            
            return True, {
                'id': user_id,
                'username': username,
                'email': email,
                'role': role,
                'is_active': bool(is_active)
            }
            
        except Exception as e:
            return False, str(e)
        finally:
            conn.close()
    
    def create_session(self, user_id, ip_address=None, user_agent=None):
        """Create a new user session"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Generate session token
            session_token = secrets.token_urlsafe(32)
            expires_at = datetime.now() + timedelta(hours=24)
            
            # Create session
            cursor.execute('''
                INSERT INTO user_sessions (user_id, session_token, expires_at, ip_address, user_agent)
                VALUES (?, ?, ?, ?, ?)
            ''', (user_id, session_token, expires_at.isoformat(), ip_address, user_agent))
            
            conn.commit()
            return session_token
            
        except Exception as e:
            print(f"Error creating session: {e}")
            return None
        finally:
            conn.close()
    
    def validate_session(self, session_token):
        """Validate user session"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT s.user_id, s.expires_at, u.username, u.email, u.role, u.is_active
                FROM user_sessions s
                JOIN users u ON s.user_id = u.id
                WHERE s.session_token = ? AND s.is_active = 1 AND s.expires_at > ?
            ''', (session_token, datetime.now().isoformat()))
            
            session_data = cursor.fetchone()
            if not session_data:
                return None
            
            user_id, expires_at, username, email, role, is_active = session_data
            
            # Check if user is still active
            if not is_active:
                return None
            
            return {
                'id': user_id,
                'username': username,
                'email': email,
                'role': role,
                'is_active': bool(is_active)
            }
            
        except Exception as e:
            print(f"Error validating session: {e}")
            return None
        finally:
            conn.close()
    
    def get_user_permissions(self, user_id):
        """Get user permissions"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT permission FROM user_permissions WHERE user_id = ?
            ''', (user_id,))
            
            permissions = [row[0] for row in cursor.fetchall()]
            return permissions
            
        except Exception as e:
            print(f"Error getting permissions: {e}")
            return []
        finally:
            conn.close()
    
    def has_permission(self, user_id, permission):
        """Check if user has specific permission"""
        permissions = self.get_user_permissions(user_id)
        return permission in permissions
    
    def log_audit_event(self, user_id, action, resource, details, ip_address=None, user_agent=None):
        """Log audit event"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT INTO audit_log (user_id, action, resource, details, ip_address, user_agent)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (user_id, action, resource, details, ip_address, user_agent))
            
            conn.commit()
            
        except Exception as e:
            print(f"Error logging audit event: {e}")
        finally:
            conn.close()
    
    def get_user_by_username(self, username):
        """Get user by username"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT id, username, email, role, is_active, created_at, last_login
                FROM users WHERE username = ?
            ''', (username,))
            
            user = cursor.fetchone()
            if user:
                return {
                    'id': user[0],
                    'username': user[1],
                    'email': user[2],
                    'role': user[3],
                    'is_active': bool(user[4]),
                    'created_at': user[5],
                    'last_login': user[6]
                }
            return None
            
        except Exception as e:
            print(f"Error getting user: {e}")
            return None
        finally:
            conn.close()

# Global user manager instance
user_manager = UserManager()

def require_auth(f):
    """Decorator to require authentication"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Check for session token in headers or cookies
        session_token = request.headers.get('Authorization', '').replace('Bearer ', '')
        if not session_token:
            session_token = request.cookies.get('session_token')
        
        if not session_token:
            return jsonify({'error': 'Authentication required'}), 401
        
        # Validate session
        user = user_manager.validate_session(session_token)
        if not user:
            return jsonify({'error': 'Invalid or expired session'}), 401
        
        # Add user to request context
        request.current_user = user
        return f(*args, **kwargs)
    
    return decorated_function

def require_permission(permission):
    """Decorator to require specific permission"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not hasattr(request, 'current_user'):
                return jsonify({'error': 'Authentication required'}), 401
            
            user_id = request.current_user['id']
            if not user_manager.has_permission(user_id, permission):
                return jsonify({'error': 'Insufficient permissions'}), 403
            
            return f(*args, **kwargs)
        return decorated_function
    return decorator

def get_current_user():
    """Get current authenticated user"""
    return getattr(request, 'current_user', None)

