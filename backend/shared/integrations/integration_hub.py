"""
Integration Hub - Connectors for External Data Sources
Provides a unified interface for connecting to various external data sources and services.
"""

import json
import os
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import logging
from dataclasses import dataclass, asdict
import sqlite3
import requests
from abc import ABC, abstractmethod

# For cloud storage connectors
try:
    import boto3
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

@dataclass
class ConnectionConfig:
    """Configuration for external data source connections"""
    name: str
    type: str  # 'database', 'api', 's3', 'gcs', 'azure', 'ftp', 'webhook'
    endpoint: str
    credentials: Dict[str, Any]
    metadata: Dict[str, Any]
    created_at: datetime
    last_used: Optional[datetime] = None
    is_active: bool = True

@dataclass
class SyncJob:
    """Data synchronization job configuration"""
    job_id: str
    connection_name: str
    source_path: str
    destination_path: str
    sync_schedule: str  # cron-like: 'daily', 'hourly', 'weekly'
    filters: Dict[str, Any]
    last_sync: Optional[datetime] = None
    status: str = 'pending'  # 'pending', 'running', 'completed', 'failed'
    sync_count: int = 0

class BaseConnector(ABC):
    """Base class for all data source connectors"""
    
    def __init__(self, config: ConnectionConfig):
        self.config = config
        self.logger = logging.getLogger(f"connector.{config.type}")
    
    @abstractmethod
    def test_connection(self) -> Dict[str, Any]:
        """Test the connection to the data source"""
        pass
    
    @abstractmethod
    def list_resources(self, path: str = "") -> List[Dict[str, Any]]:
        """List available resources/files/tables"""
        pass
    
    @abstractmethod
    def fetch_data(self, resource_path: str, filters: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Fetch data from the specified resource"""
        pass
    
    @abstractmethod
    def get_schema(self, resource_path: str) -> Dict[str, Any]:
        """Get schema information for a resource"""
        pass

class APIConnector(BaseConnector):
    """Connector for REST API data sources"""
    
    def test_connection(self) -> Dict[str, Any]:
        try:
            headers = self.config.credentials.get('headers', {})
            response = requests.get(
                f"{self.config.endpoint}/health",
                headers=headers,
                timeout=10
            )
            return {
                "status": "success" if response.status_code == 200 else "failed",
                "response_time": response.elapsed.total_seconds(),
                "status_code": response.status_code
            }
        except Exception as e:
            return {"status": "failed", "error": str(e)}
    
    def list_resources(self, path: str = "") -> List[Dict[str, Any]]:
        try:
            headers = self.config.credentials.get('headers', {})
            url = f"{self.config.endpoint}/{path}" if path else self.config.endpoint
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            if isinstance(data, list):
                return [{"name": f"resource_{i}", "type": "endpoint", "data": item} for i, item in enumerate(data)]
            else:
                return [{"name": "root", "type": "endpoint", "data": data}]
        except Exception as e:
            self.logger.error(f"Failed to list API resources: {e}")
            return []
    
    def fetch_data(self, resource_path: str, filters: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        try:
            headers = self.config.credentials.get('headers', {})
            params = filters or {}
            
            url = f"{self.config.endpoint}/{resource_path}"
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            
            data = response.json()
            return data if isinstance(data, list) else [data]
        except Exception as e:
            self.logger.error(f"Failed to fetch API data: {e}")
            return []
    
    def get_schema(self, resource_path: str) -> Dict[str, Any]:
        # For APIs, we'll infer schema from a sample response
        sample_data = self.fetch_data(resource_path)
        if sample_data:
            return self._infer_schema(sample_data[0])
        return {}
    
    def _infer_schema(self, sample: Dict[str, Any]) -> Dict[str, Any]:
        schema = {}
        for key, value in sample.items():
            schema[key] = {
                "type": type(value).__name__,
                "nullable": value is None
            }
        return schema

class S3Connector(BaseConnector):
    """Connector for AWS S3 data sources"""
    
    def __init__(self, config: ConnectionConfig):
        super().__init__(config)
        if not HAS_BOTO3:
            raise ImportError("boto3 is required for S3 connector")
        
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=config.credentials.get('access_key_id'),
            aws_secret_access_key=config.credentials.get('secret_access_key'),
            region_name=config.credentials.get('region', 'us-west-2')
        )
        self.bucket = config.credentials.get('bucket')
    
    def test_connection(self) -> Dict[str, Any]:
        try:
            self.s3_client.head_bucket(Bucket=self.bucket)
            return {"status": "success", "bucket": self.bucket}
        except Exception as e:
            return {"status": "failed", "error": str(e)}
    
    def list_resources(self, path: str = "") -> List[Dict[str, Any]]:
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=path
            )
            
            objects = []
            for obj in response.get('Contents', []):
                objects.append({
                    "name": obj['Key'],
                    "type": "file",
                    "size": obj['Size'],
                    "last_modified": obj['LastModified'].isoformat()
                })
            return objects
        except Exception as e:
            self.logger.error(f"Failed to list S3 objects: {e}")
            return []
    
    def fetch_data(self, resource_path: str, filters: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=resource_path)
            content = response['Body'].read().decode('utf-8')
            
            # Try to parse as JSON
            try:
                data = json.loads(content)
                return data if isinstance(data, list) else [data]
            except json.JSONDecodeError:
                # Return as text
                return [{"content": content, "file": resource_path}]
        except Exception as e:
            self.logger.error(f"Failed to fetch S3 data: {e}")
            return []
    
    def get_schema(self, resource_path: str) -> Dict[str, Any]:
        sample_data = self.fetch_data(resource_path)
        if sample_data and isinstance(sample_data[0], dict):
            return self._infer_schema(sample_data[0])
        return {"type": "text", "format": "unknown"}
    
    def _infer_schema(self, sample: Dict[str, Any]) -> Dict[str, Any]:
        schema = {}
        for key, value in sample.items():
            schema[key] = {
                "type": type(value).__name__,
                "nullable": value is None
            }
        return schema

class IntegrationHub:
    """Central hub for managing external data source integrations"""
    
    def __init__(self, db_path: str = "data/integration_hub.db"):
        self.db_path = db_path
        self.logger = logging.getLogger("integration_hub")
        self._init_database()
        self.connectors = {}
        
        # Map connector types to classes
        self.connector_classes = {
            'api': APIConnector,
            's3': S3Connector,
            # Add more connector types as needed
        }
    
    def _init_database(self):
        """Initialize the SQLite database for storing integration configurations"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS connections (
                    name TEXT PRIMARY KEY,
                    type TEXT NOT NULL,
                    endpoint TEXT NOT NULL,
                    credentials TEXT NOT NULL,
                    metadata TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    last_used TEXT,
                    is_active BOOLEAN DEFAULT 1
                )
            ''')
            
            conn.execute('''
                CREATE TABLE IF NOT EXISTS sync_jobs (
                    job_id TEXT PRIMARY KEY,
                    connection_name TEXT NOT NULL,
                    source_path TEXT NOT NULL,
                    destination_path TEXT NOT NULL,
                    sync_schedule TEXT NOT NULL,
                    filters TEXT NOT NULL,
                    last_sync TEXT,
                    status TEXT DEFAULT 'pending',
                    sync_count INTEGER DEFAULT 0,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (connection_name) REFERENCES connections (name)
                )
            ''')
            
            conn.execute('''
                CREATE TABLE IF NOT EXISTS sync_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    status TEXT NOT NULL,
                    records_processed INTEGER DEFAULT 0,
                    error_message TEXT,
                    duration_ms INTEGER,
                    FOREIGN KEY (job_id) REFERENCES sync_jobs (job_id)
                )
            ''')
    
    def add_connection(self, config: ConnectionConfig) -> Dict[str, Any]:
        """Add a new connection configuration"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    INSERT OR REPLACE INTO connections 
                    (name, type, endpoint, credentials, metadata, created_at, is_active)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    config.name,
                    config.type,
                    config.endpoint,
                    json.dumps(config.credentials),
                    json.dumps(config.metadata),
                    config.created_at.isoformat(),
                    config.is_active
                ))
            
            self.logger.info(f"Added connection: {config.name}")
            return {"status": "success", "message": f"Connection '{config.name}' added successfully"}
        
        except Exception as e:
            self.logger.error(f"Failed to add connection: {e}")
            return {"status": "error", "message": str(e)}
    
    def get_connector(self, connection_name: str) -> Optional[BaseConnector]:
        """Get a connector instance for the specified connection"""
        if connection_name in self.connectors:
            return self.connectors[connection_name]
        
        # Load from database
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                'SELECT * FROM connections WHERE name = ? AND is_active = 1',
                (connection_name,)
            )
            row = cursor.fetchone()
            
            if not row:
                return None
            
            config = ConnectionConfig(
                name=row[0],
                type=row[1],
                endpoint=row[2],
                credentials=json.loads(row[3]),
                metadata=json.loads(row[4]),
                created_at=datetime.fromisoformat(row[5]),
                last_used=datetime.fromisoformat(row[6]) if row[6] else None,
                is_active=bool(row[7])
            )
            
            # Create connector instance
            connector_class = self.connector_classes.get(config.type)
            if connector_class:
                connector = connector_class(config)
                self.connectors[connection_name] = connector
                return connector
        
        return None
    
    def test_connection(self, connection_name: str) -> Dict[str, Any]:
        """Test a connection"""
        connector = self.get_connector(connection_name)
        if not connector:
            return {"status": "error", "message": "Connection not found"}
        
        result = connector.test_connection()
        
        # Update last_used timestamp
        if result.get("status") == "success":
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    'UPDATE connections SET last_used = ? WHERE name = ?',
                    (datetime.now().isoformat(), connection_name)
                )
        
        return result
    
    def list_connections(self) -> List[Dict[str, Any]]:
        """List all configured connections"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('SELECT * FROM connections ORDER BY created_at DESC')
            connections = []
            
            for row in cursor.fetchall():
                connections.append({
                    "name": row[0],
                    "type": row[1],
                    "endpoint": row[2],
                    "metadata": json.loads(row[4]),
                    "created_at": row[5],
                    "last_used": row[6],
                    "is_active": bool(row[7])
                })
            
            return connections
    
    def list_service_types(self) -> List[Dict[str, Any]]:
        """List available service types and their capabilities"""
        return [
            {
                "type": "api",
                "name": "REST API",
                "description": "Connect to REST API endpoints",
                "capabilities": ["read", "write", "real-time"],
                "required_credentials": ["headers", "auth_token"],
                "supported_formats": ["json", "xml"]
            },
            {
                "type": "s3", 
                "name": "Amazon S3",
                "description": "Connect to Amazon S3 storage",
                "capabilities": ["read", "write", "batch"],
                "required_credentials": ["access_key", "secret_key", "region"],
                "supported_formats": ["json", "csv", "parquet", "text"]
            },
            {
                "type": "database",
                "name": "Database",
                "description": "Connect to SQL databases",
                "capabilities": ["read", "write", "batch", "streaming"],
                "required_credentials": ["host", "port", "username", "password", "database"],
                "supported_formats": ["json", "csv"]
            },
            {
                "type": "sftp",
                "name": "SFTP",
                "description": "Secure file transfer protocol",
                "capabilities": ["read", "write", "batch"],
                "required_credentials": ["host", "port", "username", "password"],
                "supported_formats": ["json", "csv", "text", "xml"]
            }
        ]
    
    def sync_data(self, job_id: str) -> Dict[str, Any]:
        """Execute a data synchronization job"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                'SELECT * FROM sync_jobs WHERE job_id = ?',
                (job_id,)
            )
            job_row = cursor.fetchone()
            
            if not job_row:
                return {"status": "error", "message": "Sync job not found"}
            
            job = SyncJob(
                job_id=job_row[0],
                connection_name=job_row[1],
                source_path=job_row[2],
                destination_path=job_row[3],
                sync_schedule=job_row[4],
                filters=json.loads(job_row[5]),
                last_sync=datetime.fromisoformat(job_row[6]) if job_row[6] else None,
                status=job_row[7],
                sync_count=job_row[8]
            )
        
        # Get connector
        connector = self.get_connector(job.connection_name)
        if not connector:
            return {"status": "error", "message": "Connection not available"}
        
        start_time = datetime.now()
        try:
            # Update job status
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    'UPDATE sync_jobs SET status = ? WHERE job_id = ?',
                    ('running', job_id)
                )
            
            # Fetch data
            data = connector.fetch_data(job.source_path, job.filters)
            
            # Save data to destination
            os.makedirs(os.path.dirname(job.destination_path), exist_ok=True)
            with open(job.destination_path, 'w') as f:
                json.dump(data, f, indent=2, default=str)
            
            # Update job completion
            end_time = datetime.now()
            duration_ms = int((end_time - start_time).total_seconds() * 1000)
            
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    UPDATE sync_jobs 
                    SET status = ?, last_sync = ?, sync_count = sync_count + 1
                    WHERE job_id = ?
                ''', ('completed', end_time.isoformat(), job_id))
                
                # Log sync result
                conn.execute('''
                    INSERT INTO sync_logs 
                    (job_id, timestamp, status, records_processed, duration_ms)
                    VALUES (?, ?, ?, ?, ?)
                ''', (job_id, end_time.isoformat(), 'completed', len(data), duration_ms))
            
            return {
                "status": "success",
                "records_processed": len(data),
                "duration_ms": duration_ms,
                "destination": job.destination_path
            }
        
        except Exception as e:
            # Update job failure
            end_time = datetime.now()
            duration_ms = int((end_time - start_time).total_seconds() * 1000)
            
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    'UPDATE sync_jobs SET status = ? WHERE job_id = ?',
                    ('failed', job_id)
                )
                
                # Log sync failure
                conn.execute('''
                    INSERT INTO sync_logs 
                    (job_id, timestamp, status, error_message, duration_ms)
                    VALUES (?, ?, ?, ?, ?)
                ''', (job_id, end_time.isoformat(), 'failed', str(e), duration_ms))
            
            self.logger.error(f"Sync job {job_id} failed: {e}")
            return {"status": "error", "message": str(e)}
    
    def create_sync_job(self, job: SyncJob) -> Dict[str, Any]:
        """Create a new synchronization job"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    INSERT INTO sync_jobs 
                    (job_id, connection_name, source_path, destination_path, 
                     sync_schedule, filters, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    job.job_id,
                    job.connection_name,
                    job.source_path,
                    job.destination_path,
                    job.sync_schedule,
                    json.dumps(job.filters),
                    datetime.now().isoformat()
                ))
            
            return {"status": "success", "job_id": job.job_id}
        
        except Exception as e:
            self.logger.error(f"Failed to create sync job: {e}")
            return {"status": "error", "message": str(e)}
    
    def get_sync_analytics(self, days: int = 7) -> Dict[str, Any]:
        """Get synchronization analytics"""
        with sqlite3.connect(self.db_path) as conn:
            # Get sync statistics
            cursor = conn.execute('''
                SELECT status, COUNT(*) as count
                FROM sync_logs 
                WHERE timestamp > datetime('now', '-{} days')
                GROUP BY status
            '''.format(days))
            
            status_counts = dict(cursor.fetchall())
            
            # Get recent sync jobs
            cursor = conn.execute('''
                SELECT sj.job_id, sj.connection_name, sj.sync_schedule, 
                       sj.status, sj.last_sync, sj.sync_count
                FROM sync_jobs sj
                ORDER BY sj.last_sync DESC
                LIMIT 10
            ''')
            
            recent_jobs = []
            for row in cursor.fetchall():
                recent_jobs.append({
                    "job_id": row[0],
                    "connection_name": row[1],
                    "sync_schedule": row[2],
                    "status": row[3],
                    "last_sync": row[4],
                    "sync_count": row[5]
                })
            
            # Get average sync duration
            cursor = conn.execute('''
                SELECT AVG(duration_ms) as avg_duration
                FROM sync_logs 
                WHERE timestamp > datetime('now', '-{} days')
                AND status = 'completed'
            '''.format(days))
            
            avg_duration = cursor.fetchone()[0] or 0
            
            return {
                "status_counts": status_counts,
                "recent_jobs": recent_jobs,
                "avg_duration_ms": avg_duration,
                "total_connections": len(self.list_connections()),
                "period_days": days
            }
