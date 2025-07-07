"""
Analytics Database Manager - Handles all database operations for analytics
"""
import sqlite3
import json
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta

class AnalyticsDatabase:
    """Manages database operations for analytics storage and retrieval"""
    
    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.analytics_dir = self.data_dir / "analytics"
        self.analytics_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.analytics_dir / "analytics.db"
        self._init_database()
    
    def _init_database(self):
        """Initialize analytics database with required tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Metrics table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                value REAL NOT NULL,
                timestamp TEXT NOT NULL,
                job_id TEXT,
                metadata TEXT
            )
        """)
        
        # Performance insights table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS insights (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                type TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT NOT NULL,
                severity TEXT NOT NULL,
                action_items TEXT NOT NULL,
                confidence REAL NOT NULL,
                created_at TEXT NOT NULL,
                is_resolved BOOLEAN DEFAULT FALSE
            )
        """)
        
        # Analytics snapshots table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS analytics_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                time_period TEXT NOT NULL,
                snapshot_data TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        """)
        
        conn.commit()
        conn.close()
    
    def store_metrics(self, analytics: Dict[str, Any]):
        """Store analytics metrics in the database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            timestamp = datetime.now().isoformat()
            
            # Store performance metrics
            if analytics.get("performance_metrics"):
                for metric_name, value in analytics["performance_metrics"].items():
                    if isinstance(value, (int, float)):
                        cursor.execute("""
                            INSERT INTO metrics (name, value, timestamp, metadata)
                            VALUES (?, ?, ?, ?)
                        """, (f"performance_{metric_name}", value, timestamp, json.dumps({"category": "performance"})))
            
            # Store quality metrics
            if analytics.get("quality_metrics"):
                for metric_name, value in analytics["quality_metrics"].items():
                    if isinstance(value, (int, float)):
                        cursor.execute("""
                            INSERT INTO metrics (name, value, timestamp, metadata)
                            VALUES (?, ?, ?, ?)
                        """, (f"quality_{metric_name}", value, timestamp, json.dumps({"category": "quality"})))
                    elif isinstance(value, dict):
                        for submetric, subvalue in value.items():
                            if isinstance(subvalue, (int, float)):
                                cursor.execute("""
                                    INSERT INTO metrics (name, value, timestamp, metadata)
                                    VALUES (?, ?, ?, ?)
                                """, (f"quality_{metric_name}_{submetric}", subvalue, timestamp, json.dumps({"category": "quality"})))
            
            # Store model analytics
            if analytics.get("model_analytics"):
                model_data = analytics["model_analytics"]
                if model_data.get("total_models_used"):
                    cursor.execute("""
                        INSERT INTO metrics (name, value, timestamp, metadata)
                        VALUES (?, ?, ?, ?)
                    """, ("model_total_models_used", model_data["total_models_used"], timestamp, json.dumps({"category": "model"})))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            print(f"Error storing metrics: {e}")
    
    def store_analytics_snapshot(self, analytics: Dict[str, Any], time_period: str):
        """Store complete analytics snapshot"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            timestamp = datetime.now().isoformat()
            snapshot_data = json.dumps(analytics, default=str)
            
            cursor.execute("""
                INSERT INTO analytics_snapshots (time_period, snapshot_data, created_at)
                VALUES (?, ?, ?)
            """, (time_period, snapshot_data, timestamp))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            print(f"Error storing analytics snapshot: {e}")
    
    def get_historical_metrics(self, metric_name: str, days: int = 30) -> List[Dict[str, Any]]:
        """Get historical metrics from the database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            start_date = datetime.now() - timedelta(days=days)
            
            cursor.execute("""
                SELECT name, value, timestamp, job_id, metadata
                FROM metrics
                WHERE name LIKE ? AND timestamp >= ?
                ORDER BY timestamp DESC
            """, (f"%{metric_name}%", start_date.isoformat()))
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    "name": row[0],
                    "value": row[1],
                    "timestamp": row[2],
                    "job_id": row[3],
                    "metadata": json.loads(row[4]) if row[4] else {}
                })
            
            conn.close()
            return results
            
        except Exception as e:
            print(f"Error getting historical metrics: {e}")
            return []
    
    def get_latest_analytics_snapshot(self, time_period: str) -> Optional[Dict[str, Any]]:
        """Get the latest analytics snapshot for a time period"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT snapshot_data, created_at
                FROM analytics_snapshots
                WHERE time_period = ?
                ORDER BY created_at DESC
                LIMIT 1
            """, (time_period,))
            
            result = cursor.fetchone()
            conn.close()
            
            if result:
                return {
                    "data": json.loads(result[0]),
                    "created_at": result[1]
                }
            
            return None
            
        except Exception as e:
            print(f"Error getting latest analytics snapshot: {e}")
            return None
    
    def store_insights(self, insights: List[Dict[str, Any]]):
        """Store performance insights"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            timestamp = datetime.now().isoformat()
            
            for insight in insights:
                cursor.execute("""
                    INSERT INTO insights (type, title, description, severity, action_items, confidence, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    insight.get("type", "general"),
                    insight.get("title", ""),
                    insight.get("description", ""),
                    insight.get("severity", "info"),
                    json.dumps(insight.get("action_items", [])),
                    insight.get("confidence", 0.0),
                    timestamp
                ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            print(f"Error storing insights: {e}")
    
    def get_recent_insights(self, days: int = 7) -> List[Dict[str, Any]]:
        """Get recent insights from the database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            start_date = datetime.now() - timedelta(days=days)
            
            cursor.execute("""
                SELECT type, title, description, severity, action_items, confidence, created_at, is_resolved
                FROM insights
                WHERE created_at >= ?
                ORDER BY created_at DESC
            """, (start_date.isoformat(),))
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    "type": row[0],
                    "title": row[1],
                    "description": row[2],
                    "severity": row[3],
                    "action_items": json.loads(row[4]) if row[4] else [],
                    "confidence": row[5],
                    "created_at": row[6],
                    "is_resolved": bool(row[7])
                })
            
            conn.close()
            return results
            
        except Exception as e:
            print(f"Error getting recent insights: {e}")
            return []
