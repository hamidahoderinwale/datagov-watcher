"""
Authentication API - User management and authentication endpoints
"""

from flask import Blueprint, request, jsonify, make_response
from datetime import datetime, timedelta
import jwt
from ..auth.authentication import user_manager, require_auth, require_permission

auth_bp = Blueprint('auth', __name__, url_prefix='/api/auth')

# JWT secret key (in production, use environment variable)
JWT_SECRET_KEY = 'your-secret-key-change-in-production'

@auth_bp.route('/login', methods=['POST'])
def login():
    """User login endpoint"""
    try:
        data = request.get_json()
        username = data.get('username')
        password = data.get('password')
        
        if not username or not password:
            return jsonify({'error': 'Username and password required'}), 400
        
        # Authenticate user
        success, result = user_manager.authenticate_user(
            username, 
            password, 
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        if not success:
            return jsonify({'error': result}), 401
        
        # Create session
        session_token = user_manager.create_session(
            result['id'],
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        if not session_token:
            return jsonify({'error': 'Failed to create session'}), 500
        
        # Create JWT token
        jwt_payload = {
            'user_id': result['id'],
            'username': result['username'],
            'role': result['role'],
            'exp': datetime.utcnow() + timedelta(hours=24)
        }
        
        jwt_token = jwt.encode(jwt_payload, JWT_SECRET_KEY, algorithm='HS256')
        
        # Create response
        response = make_response(jsonify({
            'message': 'Login successful',
            'user': {
                'id': result['id'],
                'username': result['username'],
                'email': result['email'],
                'role': result['role']
            },
            'session_token': session_token,
            'jwt_token': jwt_token
        }))
        
        # Set session cookie
        response.set_cookie(
            'session_token',
            session_token,
            max_age=24*60*60,  # 24 hours
            httponly=True,
            secure=False,  # Set to True in production with HTTPS
            samesite='Lax'
        )
        
        return response
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@auth_bp.route('/logout', methods=['POST'])
@require_auth
def logout():
    """User logout endpoint"""
    try:
        session_token = request.headers.get('Authorization', '').replace('Bearer ', '')
        if not session_token:
            session_token = request.cookies.get('session_token')
        
        if session_token:
            # Invalidate session
            conn = sqlite3.connect('datasets.db')
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE user_sessions 
                SET is_active = 0 
                WHERE session_token = ?
            ''', (session_token,))
            conn.commit()
            conn.close()
            
            # Log logout
            user_manager.log_audit_event(
                request.current_user['id'],
                'logout',
                'authentication',
                f'User {request.current_user["username"]} logged out',
                request.remote_addr,
                request.headers.get('User-Agent')
            )
        
        response = make_response(jsonify({'message': 'Logout successful'}))
        response.set_cookie('session_token', '', expires=0)
        return response
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@auth_bp.route('/register', methods=['POST'])
def register():
    """User registration endpoint"""
    try:
        data = request.get_json()
        username = data.get('username')
        email = data.get('email')
        password = data.get('password')
        role = data.get('role', 'user')
        
        if not username or not email or not password:
            return jsonify({'error': 'Username, email, and password required'}), 400
        
        # Validate password strength
        if len(password) < 8:
            return jsonify({'error': 'Password must be at least 8 characters long'}), 400
        
        # Create user
        success, message = user_manager.create_user(username, email, password, role)
        
        if not success:
            return jsonify({'error': message}), 400
        
        return jsonify({'message': message}), 201
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@auth_bp.route('/profile', methods=['GET'])
@require_auth
def get_profile():
    """Get user profile"""
    try:
        user = request.current_user
        permissions = user_manager.get_user_permissions(user['id'])
        
        return jsonify({
            'user': user,
            'permissions': permissions
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@auth_bp.route('/profile', methods=['PUT'])
@require_auth
def update_profile():
    """Update user profile"""
    try:
        data = request.get_json()
        user_id = request.current_user['id']
        
        # Update user profile (implement as needed)
        # This is a placeholder - implement actual profile update logic
        
        return jsonify({'message': 'Profile updated successfully'})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@auth_bp.route('/change-password', methods=['POST'])
@require_auth
def change_password():
    """Change user password"""
    try:
        data = request.get_json()
        current_password = data.get('current_password')
        new_password = data.get('new_password')
        
        if not current_password or not new_password:
            return jsonify({'error': 'Current and new password required'}), 400
        
        if len(new_password) < 8:
            return jsonify({'error': 'New password must be at least 8 characters long'}), 400
        
        # Verify current password
        user_id = request.current_user['id']
        conn = sqlite3.connect('datasets.db')
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT password_hash, salt FROM users WHERE id = ?
        ''', (user_id,))
        
        user_data = cursor.fetchone()
        if not user_data:
            return jsonify({'error': 'User not found'}), 404
        
        password_hash, salt = user_data
        
        if not user_manager.verify_password(current_password, password_hash, salt):
            return jsonify({'error': 'Current password is incorrect'}), 400
        
        # Update password
        new_password_hash, new_salt = user_manager.hash_password(new_password)
        
        cursor.execute('''
            UPDATE users 
            SET password_hash = ?, salt = ?
            WHERE id = ?
        ''', (new_password_hash, new_salt, user_id))
        
        conn.commit()
        conn.close()
        
        # Log password change
        user_manager.log_audit_event(
            user_id,
            'change_password',
            'profile',
            'Password changed',
            request.remote_addr,
            request.headers.get('User-Agent')
        )
        
        return jsonify({'message': 'Password changed successfully'})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@auth_bp.route('/users', methods=['GET'])
@require_auth
@require_permission('manage_users')
def get_users():
    """Get all users (admin only)"""
    try:
        conn = sqlite3.connect('datasets.db')
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT id, username, email, role, is_active, created_at, last_login
            FROM users
            ORDER BY created_at DESC
        ''')
        
        users = []
        for row in cursor.fetchall():
            users.append({
                'id': row[0],
                'username': row[1],
                'email': row[2],
                'role': row[3],
                'is_active': bool(row[4]),
                'created_at': row[5],
                'last_login': row[6]
            })
        
        conn.close()
        
        return jsonify({'users': users})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@auth_bp.route('/users/<int:user_id>', methods=['PUT'])
@require_auth
@require_permission('manage_users')
def update_user(user_id):
    """Update user (admin only)"""
    try:
        data = request.get_json()
        
        # Update user (implement as needed)
        # This is a placeholder - implement actual user update logic
        
        return jsonify({'message': 'User updated successfully'})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@auth_bp.route('/users/<int:user_id>/permissions', methods=['GET'])
@require_auth
@require_permission('manage_users')
def get_user_permissions(user_id):
    """Get user permissions (admin only)"""
    try:
        permissions = user_manager.get_user_permissions(user_id)
        return jsonify({'permissions': permissions})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@auth_bp.route('/users/<int:user_id>/permissions', methods=['POST'])
@require_auth
@require_permission('manage_users')
def grant_permission(user_id):
    """Grant permission to user (admin only)"""
    try:
        data = request.get_json()
        permission = data.get('permission')
        
        if not permission:
            return jsonify({'error': 'Permission required'}), 400
        
        conn = sqlite3.connect('datasets.db')
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR IGNORE INTO user_permissions (user_id, permission, granted_by)
            VALUES (?, ?, ?)
        ''', (user_id, permission, request.current_user['id']))
        
        conn.commit()
        conn.close()
        
        # Log permission grant
        user_manager.log_audit_event(
            request.current_user['id'],
            'grant_permission',
            'user_management',
            f'Granted permission {permission} to user {user_id}',
            request.remote_addr,
            request.headers.get('User-Agent')
        )
        
        return jsonify({'message': 'Permission granted successfully'})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@auth_bp.route('/users/<int:user_id>/permissions', methods=['DELETE'])
@require_auth
@require_permission('manage_users')
def revoke_permission(user_id):
    """Revoke permission from user (admin only)"""
    try:
        data = request.get_json()
        permission = data.get('permission')
        
        if not permission:
            return jsonify({'error': 'Permission required'}), 400
        
        conn = sqlite3.connect('datasets.db')
        cursor = conn.cursor()
        
        cursor.execute('''
            DELETE FROM user_permissions 
            WHERE user_id = ? AND permission = ?
        ''', (user_id, permission))
        
        conn.commit()
        conn.close()
        
        # Log permission revocation
        user_manager.log_audit_event(
            request.current_user['id'],
            'revoke_permission',
            'user_management',
            f'Revoked permission {permission} from user {user_id}',
            request.remote_addr,
            request.headers.get('User-Agent')
        )
        
        return jsonify({'message': 'Permission revoked successfully'})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@auth_bp.route('/audit-log', methods=['GET'])
@require_auth
@require_permission('view_audit_log')
def get_audit_log():
    """Get audit log (admin only)"""
    try:
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 50, type=int)
        offset = (page - 1) * per_page
        
        conn = sqlite3.connect('datasets.db')
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT al.id, al.user_id, u.username, al.action, al.resource, 
                   al.details, al.ip_address, al.timestamp
            FROM audit_log al
            LEFT JOIN users u ON al.user_id = u.id
            ORDER BY al.timestamp DESC
            LIMIT ? OFFSET ?
        ''', (per_page, offset))
        
        logs = []
        for row in cursor.fetchall():
            logs.append({
                'id': row[0],
                'user_id': row[1],
                'username': row[2],
                'action': row[3],
                'resource': row[4],
                'details': row[5],
                'ip_address': row[6],
                'timestamp': row[7]
            })
        
        # Get total count
        cursor.execute('SELECT COUNT(*) FROM audit_log')
        total = cursor.fetchone()[0]
        
        conn.close()
        
        return jsonify({
            'logs': logs,
            'total': total,
            'page': page,
            'per_page': per_page,
            'pages': (total + per_page - 1) // per_page
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@auth_bp.route('/verify', methods=['GET'])
@require_auth
def verify_token():
    """Verify JWT token"""
    try:
        return jsonify({
            'valid': True,
            'user': request.current_user
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

