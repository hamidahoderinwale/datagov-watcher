/**
 * Notification System JavaScript
 * Handles real-time notifications for the Dataset State Historian
 */

class NotificationManager {
    constructor() {
        this.notifications = [];
        this.isOpen = false;
        this.socket = null;
        this.init();
    }

    init() {
        this.setupSocketIO();
        this.bindEvents();
        this.loadNotifications();
        
        // Auto-refresh notifications every 30 seconds
        setInterval(() => {
            if (!this.isOpen) {
                this.loadNotifications();
            }
        }, 30000);
    }

    setupSocketIO() {
        if (typeof io !== 'undefined') {
            this.socket = io();
            this.socket.on('notification', (data) => {
                this.addNotification(data);
            });
        }
    }

    bindEvents() {
        // Make functions globally available
        window.toggleNotifications = () => this.toggle();
        window.clearAllNotifications = () => this.clearAll();
        window.refreshNotifications = () => this.refresh();
        window.dismissNotification = (id) => this.dismiss(id);
    }

    async loadNotifications() {
        try {
            const response = await fetch('/api/notifications/');
            const data = await response.json();
            
            if (data.success) {
                this.notifications = data.notifications || [];
                this.updateUI();
            } else {
                console.error('Failed to load notifications:', data.error);
                this.showError('Failed to load notifications');
            }
        } catch (error) {
            console.error('Error loading notifications:', error);
            this.showError('Connection error');
        }
    }

    updateUI() {
        this.updateBadge();
        this.updateList();
    }

    updateBadge() {
        const badge = document.getElementById('notification-badge');
        const unreadCount = this.notifications.filter(n => !n.read).length;
        
        if (badge) {
            if (unreadCount > 0) {
                badge.textContent = unreadCount > 99 ? '99+' : unreadCount;
                badge.style.display = 'flex';
            } else {
                badge.style.display = 'none';
            }
        }
    }

    updateList() {
        const list = document.getElementById('notification-list');
        if (!list) return;

        if (this.notifications.length === 0) {
            list.innerHTML = `
                <div class="notification-empty">
                    <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
                        <path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"></path>
                        <path d="M13.73 21a2 2 0 0 1-3.46 0"></path>
                    </svg>
                    <h4>No notifications</h4>
                    <p>You're all caught up! Check back later for updates.</p>
                </div>
            `;
            return;
        }

        const notificationHTML = this.notifications.map(notification => {
            const timeAgo = this.formatTimeAgo(notification.timestamp);
            const typeClass = this.getTypeClass(notification.type);
            const icon = this.getTypeIcon(notification.type);
            
            return `
                <div class="notification-item ${typeClass} ${notification.read ? '' : 'new'}" data-id="${notification.id}">
                    <div class="notification-icon">${icon}</div>
                    <div class="notification-content">
                        <div class="notification-title">${notification.title}</div>
                        <div class="notification-text">${notification.message}</div>
                        <div class="notification-meta">
                            <span class="notification-time">${timeAgo}</span>
                            ${notification.dataset_id ? `<span class="notification-dataset">Dataset: ${notification.dataset_id}</span>` : ''}
                        </div>
                        ${this.getNotificationActions(notification)}
                    </div>
                    <button class="notification-dismiss" onclick="dismissNotification('${notification.id}')" title="Dismiss">
                        ×
                    </button>
                </div>
            `;
        }).join('');

        list.innerHTML = notificationHTML;
    }

    getTypeClass(type) {
        const typeMap = {
            'change': 'info',
            'error': 'error',
            'warning': 'warning',
            'success': 'success',
            'critical': 'critical'
        };
        return typeMap[type] || 'info';
    }

    getTypeIcon(type) {
        const icons = {
            'change': `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <path d="M3 12a9 9 0 0 1 9-9 9.75 9.75 0 0 1 6.74 2.74L21 8"></path>
                <path d="M21 3v5h-5"></path>
                <path d="M21 12a9 9 0 0 1-9 9 9.75 9.75 0 0 1-6.74-2.74L3 16"></path>
                <path d="M3 21v-5h5"></path>
            </svg>`,
            'error': `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <circle cx="12" cy="12" r="10"></circle>
                <line x1="15" y1="9" x2="9" y2="15"></line>
                <line x1="9" y1="9" x2="15" y2="15"></line>
            </svg>`,
            'warning': `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3Z"></path>
                <path d="M12 9v4"></path>
                <path d="m12 17 .01 0"></path>
            </svg>`,
            'success': `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"></path>
                <polyline points="22,4 12,14.01 9,11.01"></polyline>
            </svg>`
        };
        return icons[type] || icons['change'];
    }

    getNotificationActions(notification) {
        if (notification.type === 'change' && notification.dataset_id) {
            return `
                <div class="notification-actions">
                    <button class="notification-action primary" onclick="window.location.href='/dataset/${notification.dataset_id}'">
                        View Dataset
                    </button>
                    <button class="notification-action" onclick="window.location.href='/dataset/${notification.dataset_id}/changes'">
                        View Changes
                    </button>
                </div>
            `;
        }
        return '';
    }

    formatTimeAgo(timestamp) {
        const now = new Date();
        const time = new Date(timestamp);
        const diffMs = now - time;
        const diffMins = Math.floor(diffMs / 60000);
        const diffHours = Math.floor(diffMins / 60);
        const diffDays = Math.floor(diffHours / 24);

        if (diffMins < 1) return 'Just now';
        if (diffMins < 60) return `${diffMins}m ago`;
        if (diffHours < 24) return `${diffHours}h ago`;
        if (diffDays < 7) return `${diffDays}d ago`;
        return time.toLocaleDateString();
    }

    toggle() {
        const panel = document.getElementById('notification-panel');
        if (!panel) return;

        this.isOpen = !this.isOpen;
        
        if (this.isOpen) {
            panel.classList.add('open');
            this.loadNotifications(); // Refresh when opened
            this.markAsRead();
        } else {
            panel.classList.remove('open');
        }
    }

    async markAsRead() {
        try {
            await fetch('/api/notifications/mark-read', { method: 'POST' });
            this.notifications.forEach(n => n.read = true);
            this.updateBadge();
        } catch (error) {
            console.error('Error marking notifications as read:', error);
        }
    }

    async clearAll() {
        try {
            const response = await fetch('/api/notifications/clear', { method: 'POST' });
            const data = await response.json();
            
            if (data.success) {
                this.notifications = [];
                this.updateUI();
                this.showSuccess('All notifications cleared');
            } else {
                this.showError('Failed to clear notifications');
            }
        } catch (error) {
            console.error('Error clearing notifications:', error);
            this.showError('Connection error');
        }
    }

    async refresh() {
        await this.loadNotifications();
        this.showSuccess('Notifications refreshed');
    }

    async dismiss(id) {
        try {
            const response = await fetch(`/api/notifications/${id}`, { method: 'DELETE' });
            const data = await response.json();
            
            if (data.success) {
                this.notifications = this.notifications.filter(n => n.id !== id);
                this.updateUI();
            } else {
                this.showError('Failed to dismiss notification');
            }
        } catch (error) {
            console.error('Error dismissing notification:', error);
            this.showError('Connection error');
        }
    }

    addNotification(notification) {
        this.notifications.unshift(notification);
        this.updateUI();
        
        // Show browser notification if permission granted
        if (Notification.permission === 'granted') {
            new Notification(notification.title, {
                body: notification.message,
                icon: '/static/favicon.ico'
            });
        }
    }

    showSuccess(message) {
        this.showToast(message, 'success');
    }

    showError(message) {
        this.showToast(message, 'error');
    }

    showToast(message, type) {
        // Simple toast notification
        const toast = document.createElement('div');
        toast.className = `toast toast-${type}`;
        toast.textContent = message;
        toast.style.cssText = `
            position: fixed;
            top: 20px;
            right: 20px;
            padding: 12px 20px;
            background: ${type === 'success' ? '#10b981' : '#ef4444'};
            color: white;
            border-radius: 6px;
            z-index: 10000;
            opacity: 0;
            transform: translateX(100%);
            transition: all 0.3s ease;
        `;
        
        document.body.appendChild(toast);
        
        // Animate in
        setTimeout(() => {
            toast.style.opacity = '1';
            toast.style.transform = 'translateX(0)';
        }, 10);
        
        // Remove after 3 seconds
        setTimeout(() => {
            toast.style.opacity = '0';
            toast.style.transform = 'translateX(100%)';
            setTimeout(() => toast.remove(), 300);
        }, 3000);
    }
}

// Set up notification permission request (user-initiated only)
function setupNotificationPermission() {
    if (!('Notification' in window)) {
        return; // Browser doesn't support notifications
    }
    
    // Only show permission request when user interacts with notifications
    const notificationToggle = document.getElementById('notification-toggle');
    if (notificationToggle) {
        notificationToggle.addEventListener('click', function() {
            if (Notification.permission === 'default') {
                Notification.requestPermission().then(function(permission) {
                    if (permission === 'granted') {
                        console.log('Notification permission granted');
                    }
                });
            }
        });
    }
}

// Initialize notification manager when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    window.notificationManager = new NotificationManager();
    
    // Set up notification permission request (user-initiated only)
    setupNotificationPermission();
});
