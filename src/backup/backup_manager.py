"""
Backup and Restore System
"""

import os
import sqlite3
import shutil
import gzip
import json
import tarfile
from datetime import datetime, timedelta
from pathlib import Path
import hashlib
import threading
import time

class BackupManager:
    def __init__(self, db_path='datasets.db', backup_dir='backups'):
        self.db_path = db_path
        self.backup_dir = Path(backup_dir)
        self.backup_dir.mkdir(exist_ok=True)
        self.init_backup_tables()
    
    def init_backup_tables(self):
        """Initialize backup tracking tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Backup history table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS backup_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                backup_name VARCHAR(255) NOT NULL,
                backup_type VARCHAR(50) NOT NULL,
                file_path VARCHAR(500) NOT NULL,
                file_size INTEGER NOT NULL,
                checksum VARCHAR(64) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status VARCHAR(20) DEFAULT 'completed',
                error_message TEXT NULL
            )
        ''')
        
        # Restore history table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS restore_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                backup_name VARCHAR(255) NOT NULL,
                restore_type VARCHAR(50) NOT NULL,
                restored_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status VARCHAR(20) DEFAULT 'completed',
                error_message TEXT NULL
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def create_backup(self, backup_type='full', include_data=True, compress=True):
        """Create a backup of the database and application data"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_name = f"{backup_type}_backup_{timestamp}"
            
            if backup_type == 'full':
                return self._create_full_backup(backup_name, include_data, compress)
            elif backup_type == 'incremental':
                return self._create_incremental_backup(backup_name, include_data, compress)
            elif backup_type == 'database_only':
                return self._create_database_backup(backup_name, compress)
            else:
                raise ValueError(f"Unknown backup type: {backup_type}")
                
        except Exception as e:
            self._log_backup_error(backup_name, str(e))
            raise
    
    def _create_full_backup(self, backup_name, include_data, compress):
        """Create a full backup including database and all data"""
        backup_path = self.backup_dir / f"{backup_name}.tar.gz" if compress else self.backup_dir / f"{backup_name}.tar"
        
        with tarfile.open(backup_path, 'w:gz' if compress else 'w') as tar:
            # Add database file
            if os.path.exists(self.db_path):
                tar.add(self.db_path, arcname='datasets.db')
            
            # Add application files
            app_files = [
                'unified_app.py',
                'start_app.py',
                'requirements.txt',
                'src/',
                'web/',
                'config/'
            ]
            
            for file_path in app_files:
                if os.path.exists(file_path):
                    tar.add(file_path, arcname=file_path)
            
            # Add configuration files
            config_files = [
                '.env',
                'config.json',
                'settings.yaml'
            ]
            
            for config_file in config_files:
                if os.path.exists(config_file):
                    tar.add(config_file, arcname=config_file)
        
        # Calculate file size and checksum
        file_size = backup_path.stat().st_size
        checksum = self._calculate_checksum(backup_path)
        
        # Log backup
        self._log_backup_success(backup_name, 'full', str(backup_path), file_size, checksum)
        
        return {
            'backup_name': backup_name,
            'backup_type': 'full',
            'file_path': str(backup_path),
            'file_size': file_size,
            'checksum': checksum,
            'created_at': datetime.now().isoformat()
        }
    
    def _create_incremental_backup(self, backup_name, include_data, compress):
        """Create an incremental backup of changes since last backup"""
        # Get last backup date
        last_backup = self._get_last_backup_date()
        
        backup_path = self.backup_dir / f"{backup_name}.tar.gz" if compress else self.backup_dir / f"{backup_name}.tar"
        
        with tarfile.open(backup_path, 'w:gz' if compress else 'w') as tar:
            # Add database file
            if os.path.exists(self.db_path):
                tar.add(self.db_path, arcname='datasets.db')
            
            # Add only changed files since last backup
            if last_backup:
                for root, dirs, files in os.walk('.'):
                    for file in files:
                        file_path = os.path.join(root, file)
                        if os.path.getmtime(file_path) > last_backup.timestamp():
                            tar.add(file_path, arcname=file_path)
        
        # Calculate file size and checksum
        file_size = backup_path.stat().st_size
        checksum = self._calculate_checksum(backup_path)
        
        # Log backup
        self._log_backup_success(backup_name, 'incremental', str(backup_path), file_size, checksum)
        
        return {
            'backup_name': backup_name,
            'backup_type': 'incremental',
            'file_path': str(backup_path),
            'file_size': file_size,
            'checksum': checksum,
            'created_at': datetime.now().isoformat()
        }
    
    def _create_database_backup(self, backup_name, compress):
        """Create a database-only backup"""
        backup_path = self.backup_dir / f"{backup_name}.db.gz" if compress else self.backup_dir / f"{backup_name}.db"
        
        if compress:
            with open(self.db_path, 'rb') as f_in:
                with gzip.open(backup_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        else:
            shutil.copy2(self.db_path, backup_path)
        
        # Calculate file size and checksum
        file_size = backup_path.stat().st_size
        checksum = self._calculate_checksum(backup_path)
        
        # Log backup
        self._log_backup_success(backup_name, 'database_only', str(backup_path), file_size, checksum)
        
        return {
            'backup_name': backup_name,
            'backup_type': 'database_only',
            'file_path': str(backup_path),
            'file_size': file_size,
            'checksum': checksum,
            'created_at': datetime.now().isoformat()
        }
    
    def restore_backup(self, backup_name, restore_type='full'):
        """Restore from a backup"""
        try:
            # Find backup file
            backup_file = self._find_backup_file(backup_name)
            if not backup_file:
                raise FileNotFoundError(f"Backup file not found: {backup_name}")
            
            if restore_type == 'full':
                return self._restore_full_backup(backup_file, backup_name)
            elif restore_type == 'database_only':
                return self._restore_database_backup(backup_file, backup_name)
            else:
                raise ValueError(f"Unknown restore type: {restore_type}")
                
        except Exception as e:
            self._log_restore_error(backup_name, restore_type, str(e))
            raise
    
    def _restore_full_backup(self, backup_file, backup_name):
        """Restore a full backup"""
        # Create restore directory
        restore_dir = Path('restore_temp')
        restore_dir.mkdir(exist_ok=True)
        
        try:
            # Extract backup
            with tarfile.open(backup_file, 'r:*') as tar:
                tar.extractall(restore_dir)
            
            # Restore database
            db_backup = restore_dir / 'datasets.db'
            if db_backup.exists():
                shutil.copy2(db_backup, self.db_path)
            
            # Restore application files
            for item in restore_dir.iterdir():
                if item.name != 'datasets.db':
                    if item.is_file():
                        shutil.copy2(item, item.name)
                    elif item.is_dir():
                        if os.path.exists(item.name):
                            shutil.rmtree(item.name)
                        shutil.copytree(item, item.name)
            
            # Log successful restore
            self._log_restore_success(backup_name, 'full')
            
            return {
                'backup_name': backup_name,
                'restore_type': 'full',
                'restored_at': datetime.now().isoformat(),
                'status': 'completed'
            }
            
        finally:
            # Clean up restore directory
            shutil.rmtree(restore_dir, ignore_errors=True)
    
    def _restore_database_backup(self, backup_file, backup_name):
        """Restore a database-only backup"""
        if backup_file.endswith('.gz'):
            with gzip.open(backup_file, 'rb') as f_in:
                with open(self.db_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        else:
            shutil.copy2(backup_file, self.db_path)
        
        # Log successful restore
        self._log_restore_success(backup_name, 'database_only')
        
        return {
            'backup_name': backup_name,
            'restore_type': 'database_only',
            'restored_at': datetime.now().isoformat(),
            'status': 'completed'
        }
    
    def list_backups(self, limit=50):
        """List available backups"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT backup_name, backup_type, file_path, file_size, checksum, created_at, status
                FROM backup_history
                ORDER BY created_at DESC
                LIMIT ?
            ''', (limit,))
            
            backups = []
            for row in cursor.fetchall():
                backup_name, backup_type, file_path, file_size, checksum, created_at, status = row
                
                # Check if file still exists
                file_exists = os.path.exists(file_path)
                
                backups.append({
                    'backup_name': backup_name,
                    'backup_type': backup_type,
                    'file_path': file_path,
                    'file_size': file_size,
                    'checksum': checksum,
                    'created_at': created_at,
                    'status': status,
                    'file_exists': file_exists
                })
            
            return backups
            
        finally:
            conn.close()
    
    def delete_backup(self, backup_name):
        """Delete a backup"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Get backup info
            cursor.execute('''
                SELECT file_path FROM backup_history WHERE backup_name = ?
            ''', (backup_name,))
            
            result = cursor.fetchone()
            if not result:
                raise FileNotFoundError(f"Backup not found: {backup_name}")
            
            file_path = result[0]
            
            # Delete file
            if os.path.exists(file_path):
                os.remove(file_path)
            
            # Update database
            cursor.execute('''
                UPDATE backup_history 
                SET status = 'deleted' 
                WHERE backup_name = ?
            ''', (backup_name,))
            
            conn.commit()
            
            return {'message': f'Backup {backup_name} deleted successfully'}
            
        finally:
            conn.close()
    
    def cleanup_old_backups(self, days_to_keep=30):
        """Clean up old backups"""
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Get old backups
            cursor.execute('''
                SELECT backup_name, file_path FROM backup_history
                WHERE created_at < ? AND status = 'completed'
            ''', (cutoff_date.isoformat(),))
            
            deleted_count = 0
            for backup_name, file_path in cursor.fetchall():
                try:
                    if os.path.exists(file_path):
                        os.remove(file_path)
                    
                    cursor.execute('''
                        UPDATE backup_history 
                        SET status = 'deleted' 
                        WHERE backup_name = ?
                    ''', (backup_name,))
                    
                    deleted_count += 1
                except Exception as e:
                    print(f"Error deleting backup {backup_name}: {e}")
            
            conn.commit()
            return {'deleted_count': deleted_count, 'cutoff_date': cutoff_date.isoformat()}
            
        finally:
            conn.close()
    
    def _calculate_checksum(self, file_path):
        """Calculate SHA-256 checksum of file"""
        hash_sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_sha256.update(chunk)
        return hash_sha256.hexdigest()
    
    def _find_backup_file(self, backup_name):
        """Find backup file by name"""
        # Try different extensions
        extensions = ['.tar.gz', '.tar', '.db.gz', '.db']
        
        for ext in extensions:
            file_path = self.backup_dir / f"{backup_name}{ext}"
            if file_path.exists():
                return str(file_path)
        
        return None
    
    def _get_last_backup_date(self):
        """Get date of last successful backup"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT created_at FROM backup_history
                WHERE status = 'completed'
                ORDER BY created_at DESC
                LIMIT 1
            ''')
            
            result = cursor.fetchone()
            if result:
                return datetime.fromisoformat(result[0])
            return None
            
        finally:
            conn.close()
    
    def _log_backup_success(self, backup_name, backup_type, file_path, file_size, checksum):
        """Log successful backup"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT INTO backup_history (backup_name, backup_type, file_path, file_size, checksum, status)
                VALUES (?, ?, ?, ?, ?, 'completed')
            ''', (backup_name, backup_type, file_path, file_size, checksum))
            conn.commit()
        finally:
            conn.close()
    
    def _log_backup_error(self, backup_name, error_message):
        """Log backup error"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT INTO backup_history (backup_name, backup_type, file_path, file_size, checksum, status, error_message)
                VALUES (?, 'unknown', '', 0, '', 'failed', ?)
            ''', (backup_name, error_message))
            conn.commit()
        finally:
            conn.close()
    
    def _log_restore_success(self, backup_name, restore_type):
        """Log successful restore"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT INTO restore_history (backup_name, restore_type, status)
                VALUES (?, ?, 'completed')
            ''', (backup_name, restore_type))
            conn.commit()
        finally:
            conn.close()
    
    def _log_restore_error(self, backup_name, restore_type, error_message):
        """Log restore error"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT INTO restore_history (backup_name, restore_type, status, error_message)
                VALUES (?, ?, 'failed', ?)
            ''', (backup_name, restore_type, error_message))
            conn.commit()
        finally:
            conn.close()

# Global backup manager instance
backup_manager = BackupManager()

class BackupScheduler:
    def __init__(self, backup_manager):
        self.backup_manager = backup_manager
        self.running = False
        self.thread = None
    
    def start_scheduler(self):
        """Start the backup scheduler"""
        if not self.running:
            self.running = True
            self.thread = threading.Thread(target=self._scheduler_loop, daemon=True)
            self.thread.start()
            print("Backup scheduler started")
    
    def stop_scheduler(self):
        """Stop the backup scheduler"""
        self.running = False
        if self.thread:
            self.thread.join()
        print("Backup scheduler stopped")
    
    def _scheduler_loop(self):
        """Main scheduler loop"""
        while self.running:
            try:
                current_time = datetime.now()
                
                # Daily full backup at 2 AM
                if current_time.hour == 2 and current_time.minute == 0:
                    self.backup_manager.create_backup('full', include_data=True, compress=True)
                    print("Daily full backup completed")
                
                # Hourly incremental backup
                elif current_time.minute == 0:
                    self.backup_manager.create_backup('incremental', include_data=True, compress=True)
                    print("Hourly incremental backup completed")
                
                # Cleanup old backups weekly
                elif current_time.weekday() == 0 and current_time.hour == 3 and current_time.minute == 0:
                    result = self.backup_manager.cleanup_old_backups(days_to_keep=30)
                    print(f"Backup cleanup completed: {result['deleted_count']} backups deleted")
                
                time.sleep(60)  # Check every minute
                
            except Exception as e:
                print(f"Backup scheduler error: {e}")
                time.sleep(300)  # Wait 5 minutes on error

# Global backup scheduler instance
backup_scheduler = BackupScheduler(backup_manager)

