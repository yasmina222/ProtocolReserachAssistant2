"""
Protocol Education CI System - Caching Module
Implements intelligent caching to reduce API costs and improve performance
"""

import json
import sqlite3
import hashlib
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
import logging
from pathlib import Path

from config import CACHE_DIR, CACHE_TTL_HOURS, ENABLE_CACHE

logger = logging.getLogger(__name__)

class IntelligenceCache:
    """SQLite-based cache for school intelligence data"""
    
    def __init__(self, db_path: str = None):
        if db_path is None:
            db_path = Path(CACHE_DIR) / 'protocol_cache.db'
        
        self.db_path = db_path
        self.enabled = ENABLE_CACHE
        
        if self.enabled:
            self._init_db()
    
    def _init_db(self):
        """Initialize cache database"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS school_cache (
                    cache_key TEXT PRIMARY KEY,
                    school_name TEXT NOT NULL,
                    data_type TEXT NOT NULL,
                    data TEXT NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    expires_at TIMESTAMP NOT NULL,
                    hit_count INTEGER DEFAULT 0,
                    source_urls TEXT
                )
            ''')
            
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_school_name 
                ON school_cache(school_name)
            ''')
            
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_expires 
                ON school_cache(expires_at)
            ''')
            
            # Verification results cache
            conn.execute('''
                CREATE TABLE IF NOT EXISTS verification_cache (
                    identifier TEXT PRIMARY KEY,
                    identifier_type TEXT NOT NULL,
                    is_valid BOOLEAN NOT NULL,
                    confidence_score REAL NOT NULL,
                    verified_at TIMESTAMP NOT NULL,
                    details TEXT
                )
            ''')
            
            conn.commit()
    
    def _generate_key(self, school_name: str, data_type: str) -> str:
        """Generate cache key from school name and data type"""
        content = f"{school_name.lower()}:{data_type}"
        return hashlib.sha256(content.encode()).hexdigest()
    
    def get(self, school_name: str, data_type: str) -> Optional[Dict[str, Any]]:
        """Retrieve cached data if valid"""
        if not self.enabled:
            return None
            
        cache_key = self._generate_key(school_name, data_type)
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT data, expires_at, source_urls 
                    FROM school_cache 
                    WHERE cache_key = ? AND expires_at > ?
                ''', (cache_key, datetime.now()))
                
                row = cursor.fetchone()
                
                if row:
                    # Update hit count
                    cursor.execute('''
                        UPDATE school_cache 
                        SET hit_count = hit_count + 1 
                        WHERE cache_key = ?
                    ''', (cache_key,))
                    
                    return {
                        'data': json.loads(row['data']),
                        'source_urls': json.loads(row['source_urls']) if row['source_urls'] else [],
                        'cached': True,
                        'expires_at': row['expires_at']
                    }
                    
        except Exception as e:
            logger.error(f"Cache retrieval error: {e}")
            
        return None
    
    def set(self, school_name: str, data_type: str, data: Dict[str, Any], 
            source_urls: List[str] = None, ttl_hours: int = None):
        """Store data in cache"""
        if not self.enabled:
            return
            
        if ttl_hours is None:
            ttl_hours = CACHE_TTL_HOURS
            
        cache_key = self._generate_key(school_name, data_type)
        expires_at = datetime.now() + timedelta(hours=ttl_hours)
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    INSERT OR REPLACE INTO school_cache 
                    (cache_key, school_name, data_type, data, created_at, 
                     expires_at, source_urls, hit_count)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 
                            COALESCE((SELECT hit_count FROM school_cache WHERE cache_key = ?), 0))
                ''', (
                    cache_key, school_name, data_type, json.dumps(data),
                    datetime.now(), expires_at,
                    json.dumps(source_urls) if source_urls else None,
                    cache_key
                ))
                conn.commit()
                
        except Exception as e:
            logger.error(f"Cache storage error: {e}")
    
    def get_verification(self, identifier: str, identifier_type: str) -> Optional[Dict[str, Any]]:
        """Get cached verification result"""
        if not self.enabled:
            return None
            
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                # Verification cache expires after 7 days
                week_ago = datetime.now() - timedelta(days=7)
                
                cursor.execute('''
                    SELECT is_valid, confidence_score, details, verified_at
                    FROM verification_cache
                    WHERE identifier = ? AND identifier_type = ? AND verified_at > ?
                ''', (identifier, identifier_type, week_ago))
                
                row = cursor.fetchone()
                
                if row:
                    return {
                        'is_valid': bool(row['is_valid']),
                        'confidence_score': row['confidence_score'],
                        'details': json.loads(row['details']) if row['details'] else {},
                        'verified_at': row['verified_at'],
                        'cached': True
                    }
                    
        except Exception as e:
            logger.error(f"Verification cache retrieval error: {e}")
            
        return None
    
    def set_verification(self, identifier: str, identifier_type: str,
                        is_valid: bool, confidence_score: float, 
                        details: Dict[str, Any] = None):
        """Cache verification result"""
        if not self.enabled:
            return
            
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    INSERT OR REPLACE INTO verification_cache
                    (identifier, identifier_type, is_valid, confidence_score, 
                     verified_at, details)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    identifier, identifier_type, is_valid, confidence_score,
                    datetime.now(), json.dumps(details) if details else None
                ))
                conn.commit()
                
        except Exception as e:
            logger.error(f"Verification cache storage error: {e}")
    
    def clear_expired(self):
        """Remove expired cache entries"""
        if not self.enabled:
            return
            
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Clear expired school data
                conn.execute('''
                    DELETE FROM school_cache WHERE expires_at < ?
                ''', (datetime.now(),))
                
                # Clear old verification data
                week_ago = datetime.now() - timedelta(days=7)
                conn.execute('''
                    DELETE FROM verification_cache WHERE verified_at < ?
                ''', (week_ago,))
                
                conn.commit()
                
                # Vacuum to reclaim space
                conn.execute('VACUUM')
                
        except Exception as e:
            logger.error(f"Cache cleanup error: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        if not self.enabled:
            return {'enabled': False}
            
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Total entries
                cursor.execute('SELECT COUNT(*) FROM school_cache')
                total_entries = cursor.fetchone()[0]
                
                # Active entries
                cursor.execute('''
                    SELECT COUNT(*) FROM school_cache WHERE expires_at > ?
                ''', (datetime.now(),))
                active_entries = cursor.fetchone()[0]
                
                # Hit statistics
                cursor.execute('''
                    SELECT SUM(hit_count), AVG(hit_count), MAX(hit_count)
                    FROM school_cache
                ''')
                total_hits, avg_hits, max_hits = cursor.fetchone()
                
                # Verification cache stats
                cursor.execute('SELECT COUNT(*) FROM verification_cache')
                verification_entries = cursor.fetchone()[0]
                
                # Cache size
                cursor.execute('''
                    SELECT page_count * page_size as size 
                    FROM pragma_page_count(), pragma_page_size()
                ''')
                cache_size_bytes = cursor.fetchone()[0]
                
                return {
                    'enabled': True,
                    'total_entries': total_entries,
                    'active_entries': active_entries,
                    'expired_entries': total_entries - active_entries,
                    'total_hits': total_hits or 0,
                    'average_hits': round(avg_hits or 0, 2),
                    'max_hits': max_hits or 0,
                    'verification_entries': verification_entries,
                    'cache_size_mb': round(cache_size_bytes / (1024 * 1024), 2),
                    'hit_rate': round(
                        (total_hits or 0) / (total_entries + (total_hits or 1)), 
                        3
                    )
                }
                
        except Exception as e:
            logger.error(f"Cache stats error: {e}")
            return {'enabled': True, 'error': str(e)}