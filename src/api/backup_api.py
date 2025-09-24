"""
Backup API - Data backup and restore endpoints
"""

from flask import Blueprint, request, jsonify, send_file
from ..backup.backup_manager import backup_manager, backup_scheduler
from ..auth.authentication import require_auth, require_permission
from ..security.rate_limiting import rate_limit, security_headers
import os
from datetime import datetime

backup_bp = Blueprint('backup', __name__, url_prefix='/api/backup')

@backup_bp.route('/create', methods=['POST'])
@require_auth
@require_permission('manage_system')
@rate_limit(limit=5, window_seconds=3600)  # 5 backups per hour
@security_headers
def create_backup():
    """Create a new backup"""
    try:
        data = request.get_json() or {}
        backup_type = data.get('type', 'full')
        include_data = data.get('include_data', True)
        compress = data.get('compress', True)
        
        # Validate backup type
        valid_types = ['full', 'incremental', 'database_only']
        if backup_type not in valid_types:
            return jsonify({'error': f'Invalid backup type. Must be one of: {valid_types}'}), 400
        
        # Create backup
        result = backup_manager.create_backup(backup_type, include_data, compress)
        
        return jsonify({
            'message': 'Backup created successfully',
            'backup': result
        }), 201
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@backup_bp.route('/list')
@require_auth
@require_permission('read_analytics')
@rate_limit(limit=100, window_seconds=3600)
@security_headers
def list_backups():
    """List available backups"""
    try:
        limit = request.args.get('limit', 50, type=int)
        backups = backup_manager.list_backups(limit)
        
        return jsonify({
            'backups': backups,
            'total': len(backups)
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@backup_bp.route('/restore', methods=['POST'])
@require_auth
@require_permission('manage_system')
@rate_limit(limit=2, window_seconds=3600)  # 2 restores per hour
@security_headers
def restore_backup():
    """Restore from a backup"""
    try:
        data = request.get_json()
        backup_name = data.get('backup_name')
        restore_type = data.get('type', 'full')
        
        if not backup_name:
            return jsonify({'error': 'backup_name is required'}), 400
        
        # Validate restore type
        valid_types = ['full', 'database_only']
        if restore_type not in valid_types:
            return jsonify({'error': f'Invalid restore type. Must be one of: {valid_types}'}), 400
        
        # Restore backup
        result = backup_manager.restore_backup(backup_name, restore_type)
        
        return jsonify({
            'message': 'Backup restored successfully',
            'restore': result
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@backup_bp.route('/download/<backup_name>')
@require_auth
@require_permission('export_data')
@rate_limit(limit=10, window_seconds=3600)
@security_headers
def download_backup(backup_name):
    """Download a backup file"""
    try:
        # Find backup file
        backup_file = backup_manager._find_backup_file(backup_name)
        if not backup_file:
            return jsonify({'error': 'Backup file not found'}), 404
        
        if not os.path.exists(backup_file):
            return jsonify({'error': 'Backup file no longer exists'}), 404
        
        # Send file
        return send_file(
            backup_file,
            as_attachment=True,
            download_name=os.path.basename(backup_file)
        )
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@backup_bp.route('/delete/<backup_name>', methods=['DELETE'])
@require_auth
@require_permission('manage_system')
@rate_limit(limit=20, window_seconds=3600)
@security_headers
def delete_backup(backup_name):
    """Delete a backup"""
    try:
        result = backup_manager.delete_backup(backup_name)
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@backup_bp.route('/cleanup', methods=['POST'])
@require_auth
@require_permission('manage_system')
@rate_limit(limit=5, window_seconds=3600)
@security_headers
def cleanup_backups():
    """Clean up old backups"""
    try:
        data = request.get_json() or {}
        days_to_keep = data.get('days_to_keep', 30)
        
        result = backup_manager.cleanup_old_backups(days_to_keep)
        
        return jsonify({
            'message': 'Backup cleanup completed',
            'result': result
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@backup_bp.route('/scheduler/start', methods=['POST'])
@require_auth
@require_permission('manage_system')
@rate_limit(limit=5, window_seconds=3600)
@security_headers
def start_scheduler():
    """Start the backup scheduler"""
    try:
        backup_scheduler.start_scheduler()
        return jsonify({'message': 'Backup scheduler started'})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@backup_bp.route('/scheduler/stop', methods=['POST'])
@require_auth
@require_permission('manage_system')
@rate_limit(limit=5, window_seconds=3600)
@security_headers
def stop_scheduler():
    """Stop the backup scheduler"""
    try:
        backup_scheduler.stop_scheduler()
        return jsonify({'message': 'Backup scheduler stopped'})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@backup_bp.route('/scheduler/status')
@require_auth
@require_permission('read_analytics')
@rate_limit(limit=100, window_seconds=3600)
@security_headers
def scheduler_status():
    """Get backup scheduler status"""
    try:
        return jsonify({
            'running': backup_scheduler.running,
            'status': 'active' if backup_scheduler.running else 'stopped'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@backup_bp.route('/verify/<backup_name>')
@require_auth
@require_permission('read_analytics')
@rate_limit(limit=50, window_seconds=3600)
@security_headers
def verify_backup(backup_name):
    """Verify backup integrity"""
    try:
        # Find backup file
        backup_file = backup_manager._find_backup_file(backup_name)
        if not backup_file:
            return jsonify({'error': 'Backup file not found'}), 404
        
        if not os.path.exists(backup_file):
            return jsonify({'error': 'Backup file no longer exists'}), 404
        
        # Calculate current checksum
        current_checksum = backup_manager._calculate_checksum(backup_file)
        
        # Get stored checksum from database
        import sqlite3
        conn = sqlite3.connect('datasets.db')
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT checksum, file_size FROM backup_history 
                WHERE backup_name = ? AND status = 'completed'
            ''', (backup_name,))
            
            result = cursor.fetchone()
            if not result:
                return jsonify({'error': 'Backup record not found'}), 404
            
            stored_checksum, stored_size = result
            current_size = os.path.getsize(backup_file)
            
            # Verify checksum and size
            checksum_valid = current_checksum == stored_checksum
            size_valid = current_size == stored_size
            
            return jsonify({
                'backup_name': backup_name,
                'file_exists': True,
                'checksum_valid': checksum_valid,
                'size_valid': size_valid,
                'current_checksum': current_checksum,
                'stored_checksum': stored_checksum,
                'current_size': current_size,
                'stored_size': stored_size,
                'integrity_ok': checksum_valid and size_valid
            })
            
        finally:
            conn.close()
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@backup_bp.route('/stats')
@require_auth
@require_permission('read_analytics')
@rate_limit(limit=100, window_seconds=3600)
@security_headers
def backup_stats():
    """Get backup statistics"""
    try:
        import sqlite3
        conn = sqlite3.connect('datasets.db')
        cursor = conn.cursor()
        
        try:
            # Get backup counts by type
            cursor.execute('''
                SELECT backup_type, COUNT(*) as count, SUM(file_size) as total_size
                FROM backup_history 
                WHERE status = 'completed'
                GROUP BY backup_type
            ''')
            
            type_stats = {}
            total_size = 0
            for row in cursor.fetchall():
                backup_type, count, size = row
                type_stats[backup_type] = {
                    'count': count,
                    'total_size': size or 0
                }
                total_size += size or 0
            
            # Get recent backup activity
            cursor.execute('''
                SELECT COUNT(*) as recent_backups
                FROM backup_history 
                WHERE status = 'completed' 
                AND created_at >= datetime('now', '-7 days')
            ''')
            
            recent_backups = cursor.fetchone()[0]
            
            # Get failed backups
            cursor.execute('''
                SELECT COUNT(*) as failed_backups
                FROM backup_history 
                WHERE status = 'failed'
            ''')
            
            failed_backups = cursor.fetchone()[0]
            
            # Get restore statistics
            cursor.execute('''
                SELECT COUNT(*) as total_restores,
                       COUNT(CASE WHEN status = 'completed' THEN 1 END) as successful_restores,
                       COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_restores
                FROM restore_history
            ''')
            
            restore_stats = cursor.fetchone()
            total_restores, successful_restores, failed_restores = restore_stats
            
            return jsonify({
                'backup_stats': {
                    'by_type': type_stats,
                    'total_size': total_size,
                    'recent_backups': recent_backups,
                    'failed_backups': failed_backups
                },
                'restore_stats': {
                    'total_restores': total_restores,
                    'successful_restores': successful_restores,
                    'failed_restores': failed_restores
                },
                'scheduler_status': {
                    'running': backup_scheduler.running
                }
            })
            
        finally:
            conn.close()
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

