"""
Notifications API - Real-time notifications for dataset changes
"""

from flask import Blueprint, jsonify, request
from flask_socketio import emit
import sqlite3
import json
from datetime import datetime, timedelta
import threading
import time

notifications_bp = Blueprint('notifications', __name__, url_prefix='/api/notifications')

def get_db_connection():
    """Get database connection"""
    return sqlite3.connect('datasets.db')

@notifications_bp.route('/')
def get_notifications():
    """Get recent notifications"""
    limit = int(request.args.get('limit', 50))
    offset = int(request.args.get('offset', 0))
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get recent changes as notifications
        cursor.execute("""
            SELECT 
                ds.dataset_id,
                d.title,
                d.agency,
                ds.snapshot_date,
                ds.availability,
                ds.row_count,
                ds.column_count,
                ds.content_hash,
                'change' as type,
                'Dataset changed' as message
            FROM dataset_states ds
            JOIN datasets d ON ds.dataset_id = d.id
            WHERE ds.snapshot_date >= date('now', '-7 days')
            ORDER BY ds.snapshot_date DESC
            LIMIT ? OFFSET ?
        """, (limit, offset))
        
        notifications = []
        for row in cursor.fetchall():
            notifications.append({
                'id': f"{row[0]}_{row[3]}",
                'dataset_id': row[0],
                'title': row[1],
                'agency': row[2],
                'date': row[3],
                'status': row[4],
                'row_count': row[5],
                'column_count': row[6],
                'type': row[8],
                'message': row[9],
                'timestamp': datetime.now().isoformat()
            })
        
        conn.close()
        
        return jsonify({
            'notifications': notifications,
            'total': len(notifications),
            'unread_count': len(notifications)  # In real implementation, track read status
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@notifications_bp.route('/mark-read', methods=['POST'])
def mark_notification_read():
    """Mark notification as read"""
    notification_id = request.json.get('notification_id')
    
    # In real implementation, you'd update a read status in database
    # For now, just return success
    return jsonify({'status': 'success', 'message': 'Notification marked as read'})

@notifications_bp.route('/settings')
def get_notification_settings():
    """Get notification settings"""
    return jsonify({
        'email_notifications': True,
        'real_time_notifications': True,
        'change_threshold': 0.1,  # 10% change threshold
        'notification_types': [
            'dataset_changes',
            'availability_changes',
            'new_datasets',
            'error_events'
        ]
    })

@notifications_bp.route('/settings', methods=['POST'])
def update_notification_settings():
    """Update notification settings"""
    settings = request.json
    
    # In real implementation, you'd save settings to database
    return jsonify({'status': 'success', 'message': 'Settings updated'})

# Real-time notification system
class NotificationManager:
    def __init__(self, socketio):
        self.socketio = socketio
        self.running = False
        self.thread = None
    
    def start(self):
        """Start the notification monitoring thread"""
        if not self.running:
            self.running = True
            self.thread = threading.Thread(target=self._monitor_changes)
            self.thread.daemon = True
            self.thread.start()
    
    def stop(self):
        """Stop the notification monitoring thread"""
        self.running = False
        if self.thread:
            self.thread.join()
    
    def _monitor_changes(self):
        """Monitor for changes and send notifications"""
        last_check = datetime.now()
        
        while self.running:
            try:
                # Check for new changes since last check
                conn = get_db_connection()
                cursor = conn.cursor()
                
                cursor.execute("""
                    SELECT 
                        ds.dataset_id,
                        d.title,
                        d.agency,
                        ds.snapshot_date,
                        ds.availability,
                        ds.row_count,
                        ds.column_count,
                        ds.content_hash
                    FROM dataset_states ds
                    JOIN datasets d ON ds.dataset_id = d.id
                    WHERE ds.snapshot_date > ?
                    ORDER BY ds.snapshot_date DESC
                    LIMIT 10
                """, (last_check.strftime('%Y-%m-%d %H:%M:%S'),))
                
                changes = cursor.fetchall()
                conn.close()
                
                # Send notifications for new changes
                for change in changes:
                    notification = {
                        'id': f"{change[0]}_{change[3]}",
                        'dataset_id': change[0],
                        'title': change[1],
                        'agency': change[2],
                        'date': change[3],
                        'status': change[4],
                        'row_count': change[5],
                        'column_count': change[6],
                        'type': 'change',
                        'message': 'Dataset changed',
                        'timestamp': datetime.now().isoformat()
                    }
                    
                    # Emit to all connected clients
                    self.socketio.emit('dataset_change', notification, namespace='/')
                
                # Update last check time
                last_check = datetime.now()
                
            except Exception as e:
                print(f"Error in notification monitoring: {e}")
            
            # Wait 30 seconds before next check
            time.sleep(30)

# Global notification manager
notification_manager = None

def init_notifications(socketio):
    """Initialize the notification system"""
    global notification_manager
    notification_manager = NotificationManager(socketio)
    notification_manager.start()

def stop_notifications():
    """Stop the notification system"""
    global notification_manager
    if notification_manager:
        notification_manager.stop()

# Socket.IO event handlers
def register_notification_handlers(socketio):
    """Register Socket.IO event handlers for notifications"""
    
    @socketio.on('connect')
    def handle_connect():
        print('Client connected to notifications')
        emit('connected', {'message': 'Connected to real-time notifications'})
    
    @socketio.on('disconnect')
    def handle_disconnect():
        print('Client disconnected from notifications')
    
    @socketio.on('subscribe_notifications')
    def handle_subscribe(data):
        print(f'Client subscribed to notifications: {data}')
        emit('subscribed', {'message': 'Subscribed to notifications'})
    
    @socketio.on('unsubscribe_notifications')
    def handle_unsubscribe(data):
        print(f'Client unsubscribed from notifications: {data}')
        emit('unsubscribed', {'message': 'Unsubscribed from notifications'})

