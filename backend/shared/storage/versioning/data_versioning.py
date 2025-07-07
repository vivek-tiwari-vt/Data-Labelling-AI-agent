"""
Data Versioning & Lineage System
Tracks data transformations, label changes, and provides comprehensive audit trails
"""
import json
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import hashlib
import uuid
try:
    from git import Repo, InvalidGitRepositoryError
except ImportError:
    Repo = None
    InvalidGitRepositoryError = Exception

class ChangeType(Enum):
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    LABEL_CHANGE = "label_change"
    IMPORT = "import"
    EXPORT = "export"
    MERGE = "merge"
    SPLIT = "split"

class DataEntityType(Enum):
    DATASET = "dataset"
    TEXT_ITEM = "text_item"
    LABEL = "label"
    TEMPLATE = "template"
    JOB = "job"
    MODEL_OUTPUT = "model_output"

@dataclass
class DataVersion:
    id: str
    entity_id: str
    entity_type: DataEntityType
    version_number: int
    change_type: ChangeType
    created_at: str
    created_by: str
    data_hash: str
    content_summary: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    parent_version_id: Optional[str] = None
    change_description: str = ""

@dataclass
class LineageNode:
    id: str
    entity_id: str
    entity_type: DataEntityType
    version_id: str
    created_at: str
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class LineageEdge:
    id: str
    source_node_id: str
    target_node_id: str
    relationship_type: str
    created_at: str
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class AuditEntry:
    id: str
    entity_id: str
    entity_type: DataEntityType
    action: str
    actor: str
    timestamp: str
    details: Dict[str, Any]
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None

class DataVersioningSystem:
    """Advanced data versioning and lineage tracking system"""
    
    def __init__(self):
        self.data_dir = Path("/Volumes/DATA/Projects/data_label_agent/data/versioning")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.data_dir / "versioning.db"
        self.git_repo_path = self.data_dir / "git_repo"
        self._init_database()
        self._init_git_repo()
        
    def _init_database(self):
        """Initialize versioning database with required tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Data versions table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS data_versions (
                id TEXT PRIMARY KEY,
                entity_id TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                version_number INTEGER NOT NULL,
                change_type TEXT NOT NULL,
                created_at TEXT NOT NULL,
                created_by TEXT NOT NULL,
                data_hash TEXT NOT NULL,
                content_summary TEXT NOT NULL,
                metadata TEXT,
                parent_version_id TEXT,
                change_description TEXT,
                UNIQUE(entity_id, version_number)
            )
        """)
        
        # Lineage nodes table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS lineage_nodes (
                id TEXT PRIMARY KEY,
                entity_id TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                version_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                metadata TEXT,
                FOREIGN KEY (version_id) REFERENCES data_versions (id)
            )
        """)
        
        # Lineage edges table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS lineage_edges (
                id TEXT PRIMARY KEY,
                source_node_id TEXT NOT NULL,
                target_node_id TEXT NOT NULL,
                relationship_type TEXT NOT NULL,
                created_at TEXT NOT NULL,
                metadata TEXT,
                FOREIGN KEY (source_node_id) REFERENCES lineage_nodes (id),
                FOREIGN KEY (target_node_id) REFERENCES lineage_nodes (id)
            )
        """)
        
        # Audit log table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS audit_log (
                id TEXT PRIMARY KEY,
                entity_id TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                action TEXT NOT NULL,
                actor TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                details TEXT NOT NULL,
                ip_address TEXT,
                user_agent TEXT
            )
        """)
        
        # Data snapshots table for storing actual data
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS data_snapshots (
                version_id TEXT PRIMARY KEY,
                content TEXT NOT NULL,
                content_type TEXT NOT NULL,
                compression_type TEXT,
                FOREIGN KEY (version_id) REFERENCES data_versions (id)
            )
        """)
        
        # Create indexes for better performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_versions_entity ON data_versions(entity_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_versions_created ON data_versions(created_at)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_audit_entity ON audit_log(entity_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON audit_log(timestamp)")
        
        conn.commit()
        conn.close()
        
    def _init_git_repo(self):
        """Initialize Git repository for version control"""
        self.repo = None  # Initialize to None
        
        if Repo is None:
            return  # Git not available
            
        self.git_repo_path.mkdir(exist_ok=True)
        
        try:
            self.repo = Repo(self.git_repo_path)
        except InvalidGitRepositoryError:
            # Initialize new repository
            self.repo = Repo.init(self.git_repo_path)
            
            # Create initial commit
            readme_path = self.git_repo_path / "README.md"
            readme_path.write_text("# Data Versioning Repository\n\nThis repository tracks data versions and changes.")
            
            self.repo.index.add([str(readme_path)])
            self.repo.index.commit("Initial commit")
        except Exception as e:
            # If Git operations fail, continue without Git
            self.repo = None
    
    def create_version(self, entity_id: str, entity_type: DataEntityType, 
                      content: Any, created_by: str = "system",
                      change_type: ChangeType = ChangeType.CREATE,
                      change_description: str = "", 
                      parent_version_id: Optional[str] = None) -> str:
        """Create a new version of a data entity"""
        
        # Calculate content hash
        content_str = json.dumps(content, sort_keys=True) if not isinstance(content, str) else content
        data_hash = hashlib.sha256(content_str.encode()).hexdigest()
        
        # Get next version number
        version_number = self._get_next_version_number(entity_id)
        
        # Create version record
        version = DataVersion(
            id=str(uuid.uuid4()),
            entity_id=entity_id,
            entity_type=entity_type,
            version_number=version_number,
            change_type=change_type,
            created_at=datetime.now().isoformat(),
            created_by=created_by,
            data_hash=data_hash,
            content_summary=self._generate_content_summary(content, entity_type),
            parent_version_id=parent_version_id,
            change_description=change_description
        )
        
        # Store version and content
        self._store_version(version, content)
        
        # Create lineage node
        self._create_lineage_node(version)
        
        # Store in Git if available
        if self.repo:
            self._store_in_git(version, content)
        
        # Log audit entry
        self._log_audit_entry(
            entity_id=entity_id,
            entity_type=entity_type,
            action=f"version_{change_type.value}",
            actor=created_by,
            details={
                "version_id": version.id,
                "version_number": version_number,
                "data_hash": data_hash,
                "change_description": change_description
            }
        )
        
        return version.id
    
    def get_version_history(self, entity_id: str, limit: int = 50) -> List[DataVersion]:
        """Get version history for an entity"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT * FROM data_versions 
            WHERE entity_id = ? 
            ORDER BY version_number DESC 
            LIMIT ?
        """, (entity_id, limit))
        
        rows = cursor.fetchall()
        conn.close()
        
        return [self._row_to_version(row) for row in rows]

    def list_entities(self) -> List[Dict[str, Any]]:
        """Get all entities with their version counts"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                entity_id,
                entity_type,
                COUNT(*) as version_count,
                MAX(created_at) as last_updated
            FROM data_versions 
            GROUP BY entity_id, entity_type
            ORDER BY last_updated DESC
        """)
        
        rows = cursor.fetchall()
        conn.close()
        
        return [
            {
                "id": row[0],
                "entity_type": row[1],
                "version_count": row[2],
                "last_updated": row[3]
            }
            for row in rows
        ]

    def get_entity_versions(self, entity_id: str, limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
        """Get versions for a specific entity (for API compatibility)"""
        versions = self.get_version_history(entity_id, limit + offset)
        # Convert DataVersion objects to dictionaries for API response
        return [
            {
                "id": v.id,
                "entity_id": v.entity_id,
                "entity_type": v.entity_type.value,
                "version_number": v.version_number,
                "change_type": v.change_type.value,
                "created_at": v.created_at,
                "created_by": v.created_by,
                "data_hash": v.data_hash,
                "content_summary": v.content_summary,
                "metadata": v.metadata,
                "parent_version_id": v.parent_version_id,
                "change_description": v.change_description
            }
            for v in versions[offset:]
        ]

    def get_version(self, entity_id: str, version_id: str) -> Optional[Dict[str, Any]]:
        """Get a specific version (for API compatibility)"""
        version = self.get_version_by_id(version_id)
        if version and version.entity_id == entity_id:
            return {
                "id": version.id,
                "entity_id": version.entity_id,
                "entity_type": version.entity_type.value,
                "version_number": version.version_number,
                "change_type": version.change_type.value,
                "created_at": version.created_at,
                "created_by": version.created_by,
                "data_hash": version.data_hash,
                "content_summary": version.content_summary,
                "metadata": version.metadata,
                "parent_version_id": version.parent_version_id,
                "change_description": version.change_description
            }
        return None

    def get_lineage(self, entity_id: str) -> Dict[str, Any]:
        """Get lineage information for an entity (for API compatibility)"""
        return self.get_lineage_graph(entity_id)

    def add_lineage(self, target_entity: str, source_entity: str, relationship_type: str, metadata: Dict[str, Any] = None) -> Dict[str, Any]:
        """Add lineage relationship (for API compatibility)"""
        edge_id = self.create_lineage_relationship(source_entity, target_entity, relationship_type, metadata)
        return {"status": "success", "edge_id": edge_id}

    def get_audit_log(self, filters: Dict[str, Any], days: int, limit: int) -> List[Dict[str, Any]]:
        """Get audit log with filters (for API compatibility)"""
        audit_entries = self.get_audit_trail(
            entity_id=filters.get('entity_id'),
            entity_type=None,
            actor=filters.get('user_id'),
            time_period=f"{days}d",
            limit=limit
        )
        return [
            {
                "id": entry.id,
                "entity_id": entry.entity_id,
                "entity_type": entry.entity_type.value,
                "action": entry.action,
                "actor": entry.actor,
                "timestamp": entry.timestamp,
                "details": entry.details,
                "ip_address": entry.ip_address,
                "user_agent": entry.user_agent
            }
            for entry in audit_entries
        ]

    def compare_entity_versions(self, entity_id: str, version1: str, version2: str) -> Dict[str, Any]:
        """Compare versions with entity validation (for API compatibility)"""
        v1 = self.get_version_by_id(version1)
        v2 = self.get_version_by_id(version2)
        
        if not v1 or not v2 or v1.entity_id != entity_id or v2.entity_id != entity_id:
            return {"error": "Invalid versions for this entity"}
        
        return self.compare_versions(version1, version2)

    def rollback_entity_to_version(self, entity_id: str, target_version: str, user_id: str = "system", reason: str = "") -> Dict[str, Any]:
        """Rollback entity to specific version (for API compatibility)"""
        try:
            new_version_id = self.rollback_to_version(entity_id, target_version, user_id)
            return {"status": "success", "new_version_id": new_version_id, "reason": reason}
        except Exception as e:
            return {"error": str(e)}
    
    def get_version_content(self, version_id: str) -> Optional[Any]:
        """Get the content of a specific version"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT content, content_type FROM data_snapshots WHERE version_id = ?", 
                      (version_id,))
        row = cursor.fetchone()
        conn.close()
        
        if not row:
            return None
        
        content, content_type = row
        
        if content_type == "json":
            return json.loads(content)
        else:
            return content
    
    def compare_versions(self, version_id_1: str, version_id_2: str) -> Dict[str, Any]:
        """Compare two versions and return differences"""
        content_1 = self.get_version_content(version_id_1)
        content_2 = self.get_version_content(version_id_2)
        
        if content_1 is None or content_2 is None:
            return {"error": "One or both versions not found"}
        
        # Get version metadata
        version_1 = self.get_version_by_id(version_id_1)
        version_2 = self.get_version_by_id(version_id_2)
        
        if not version_1 or not version_2:
            return {"error": "Version metadata not found"}
        
        # Basic comparison
        comparison = {
            "version_1": {
                "id": version_1.id,
                "version_number": version_1.version_number,
                "created_at": version_1.created_at,
                "created_by": version_1.created_by,
                "data_hash": version_1.data_hash
            },
            "version_2": {
                "id": version_2.id,
                "version_number": version_2.version_number,
                "created_at": version_2.created_at,
                "created_by": version_2.created_by,
                "data_hash": version_2.data_hash
            },
            "identical": version_1.data_hash == version_2.data_hash,
            "changes": []
        }
        
        # Detailed comparison for different content types
        if isinstance(content_1, dict) and isinstance(content_2, dict):
            comparison["changes"] = self._compare_dicts(content_1, content_2)
        elif isinstance(content_1, list) and isinstance(content_2, list):
            comparison["changes"] = self._compare_lists(content_1, content_2)
        else:
            comparison["changes"] = [{
                "type": "content_change",
                "description": "Content type or structure changed"
            }]
        
        return comparison
    
    def create_lineage_relationship(self, source_entity_id: str, target_entity_id: str,
                                  relationship_type: str, metadata: Optional[Dict] = None) -> str:
        """Create a lineage relationship between two entities"""
        
        # Get the latest lineage nodes for the entities
        source_node = self._get_latest_lineage_node(source_entity_id)
        target_node = self._get_latest_lineage_node(target_entity_id)
        
        if not source_node or not target_node:
            raise ValueError("Source or target entity not found in lineage")
        
        # Create lineage edge
        edge = LineageEdge(
            id=str(uuid.uuid4()),
            source_node_id=source_node.id,
            target_node_id=target_node.id,
            relationship_type=relationship_type,
            created_at=datetime.now().isoformat(),
            metadata=metadata or {}
        )
        
        self._store_lineage_edge(edge)
        
        return edge.id
    
    def get_lineage_graph(self, entity_id: str, depth: int = 3) -> Dict[str, Any]:
        """Get the lineage graph for an entity"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Start with the entity's lineage node
        start_node = self._get_latest_lineage_node(entity_id)
        if not start_node:
            return {"nodes": [], "edges": []}
        
        visited_nodes = set()
        nodes = []
        edges = []
        
        def traverse_lineage(node_id: str, current_depth: int):
            if current_depth > depth or node_id in visited_nodes:
                return
            
            visited_nodes.add(node_id)
            
            # Get node details
            cursor.execute("SELECT * FROM lineage_nodes WHERE id = ?", (node_id,))
            node_row = cursor.fetchone()
            if node_row:
                nodes.append(self._row_to_lineage_node(node_row))
            
            # Get outgoing edges
            cursor.execute("""
                SELECT * FROM lineage_edges 
                WHERE source_node_id = ? OR target_node_id = ?
            """, (node_id, node_id))
            
            edge_rows = cursor.fetchall()
            for edge_row in edge_rows:
                edge = self._row_to_lineage_edge(edge_row)
                edges.append(edge)
                
                # Traverse to connected nodes
                next_node_id = (edge.target_node_id if edge.source_node_id == node_id 
                               else edge.source_node_id)
                traverse_lineage(next_node_id, current_depth + 1)
        
        traverse_lineage(start_node.id, 0)
        conn.close()
        
        return {
            "nodes": [self._lineage_node_to_dict(node) for node in nodes],
            "edges": [self._lineage_edge_to_dict(edge) for edge in edges],
            "root_entity_id": entity_id
        }
    
    def get_audit_trail(self, entity_id: Optional[str] = None, 
                       entity_type: Optional[DataEntityType] = None,
                       actor: Optional[str] = None,
                       time_period: str = "7d",
                       limit: int = 100) -> List[AuditEntry]:
        """Get audit trail with filtering options"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Build query conditions
        conditions = []
        params = []
        
        if entity_id:
            conditions.append("entity_id = ?")
            params.append(entity_id)
        
        if entity_type:
            conditions.append("entity_type = ?")
            params.append(entity_type.value)
        
        if actor:
            conditions.append("actor = ?")
            params.append(actor)
        
        # Time filter
        if time_period == "24h":
            start_time = datetime.now() - timedelta(hours=24)
        elif time_period == "7d":
            start_time = datetime.now() - timedelta(days=7)
        elif time_period == "30d":
            start_time = datetime.now() - timedelta(days=30)
        else:
            start_time = datetime.now() - timedelta(days=7)
        
        conditions.append("timestamp >= ?")
        params.append(start_time.isoformat())
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        cursor.execute(f"""
            SELECT * FROM audit_log 
            WHERE {where_clause}
            ORDER BY timestamp DESC 
            LIMIT ?
        """, params + [limit])
        
        rows = cursor.fetchall()
        conn.close()
        
        return [self._row_to_audit_entry(row) for row in rows]
    
    def rollback_to_version(self, entity_id: str, target_version_id: str, 
                           actor: str = "system") -> str:
        """Rollback an entity to a previous version"""
        
        # Get the target version
        target_version = self.get_version_by_id(target_version_id)
        if not target_version or target_version.entity_id != entity_id:
            raise ValueError("Target version not found or doesn't belong to entity")
        
        # Get the content of the target version
        target_content = self.get_version_content(target_version_id)
        if target_content is None:
            raise ValueError("Target version content not found")
        
        # Create new version with rolled back content
        new_version_id = self.create_version(
            entity_id=entity_id,
            entity_type=target_version.entity_type,
            content=target_content,
            created_by=actor,
            change_type=ChangeType.UPDATE,
            change_description=f"Rollback to version {target_version.version_number}",
            parent_version_id=target_version_id
        )
        
        # Log the rollback
        self._log_audit_entry(
            entity_id=entity_id,
            entity_type=target_version.entity_type,
            action="rollback",
            actor=actor,
            details={
                "target_version_id": target_version_id,
                "target_version_number": target_version.version_number,
                "new_version_id": new_version_id
            }
        )
        
        return new_version_id
    
    def get_version_by_id(self, version_id: str) -> Optional[DataVersion]:
        """Get a specific version by ID"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM data_versions WHERE id = ?", (version_id,))
        row = cursor.fetchone()
        conn.close()
        
        return self._row_to_version(row) if row else None
    
    def get_data_lineage_analytics(self, time_period: str = "30d") -> Dict[str, Any]:
        """Get analytics about data lineage and versioning"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Time filter
        if time_period == "24h":
            start_time = datetime.now() - timedelta(hours=24)
        elif time_period == "7d":
            start_time = datetime.now() - timedelta(days=7)
        elif time_period == "30d":
            start_time = datetime.now() - timedelta(days=30)
        else:
            start_time = datetime.now() - timedelta(days=30)
        
        # Version statistics
        cursor.execute("""
            SELECT 
                entity_type,
                change_type,
                COUNT(*) as count
            FROM data_versions 
            WHERE created_at >= ?
            GROUP BY entity_type, change_type
        """, (start_time.isoformat(),))
        
        version_stats = cursor.fetchall()
        
        # Audit activity
        cursor.execute("""
            SELECT 
                DATE(timestamp) as date,
                COUNT(*) as activity_count
            FROM audit_log 
            WHERE timestamp >= ?
            GROUP BY DATE(timestamp)
            ORDER BY date
        """, (start_time.isoformat(),))
        
        activity_stats = cursor.fetchall()
        
        # Top entities by version count
        cursor.execute("""
            SELECT 
                entity_id,
                entity_type,
                COUNT(*) as version_count
            FROM data_versions 
            WHERE created_at >= ?
            GROUP BY entity_id, entity_type
            ORDER BY version_count DESC
            LIMIT 10
        """, (start_time.isoformat(),))
        
        top_entities = cursor.fetchall()
        
        # Lineage complexity
        cursor.execute("SELECT COUNT(*) FROM lineage_nodes")
        total_nodes = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM lineage_edges")
        total_edges = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            "version_statistics": [
                {
                    "entity_type": row[0],
                    "change_type": row[1],
                    "count": row[2]
                }
                for row in version_stats
            ],
            "daily_activity": [
                {
                    "date": row[0],
                    "activity_count": row[1]
                }
                for row in activity_stats
            ],
            "top_versioned_entities": [
                {
                    "entity_id": row[0],
                    "entity_type": row[1],
                    "version_count": row[2]
                }
                for row in top_entities
            ],
            "lineage_complexity": {
                "total_nodes": total_nodes,
                "total_edges": total_edges,
                "avg_connections": total_edges / total_nodes if total_nodes > 0 else 0
            }
        }
    
    def _get_next_version_number(self, entity_id: str) -> int:
        """Get the next version number for an entity"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT MAX(version_number) FROM data_versions WHERE entity_id = ?
        """, (entity_id,))
        
        result = cursor.fetchone()
        conn.close()
        
        return (result[0] + 1) if result[0] is not None else 1
    
    def _generate_content_summary(self, content: Any, entity_type: DataEntityType) -> str:
        """Generate a summary of the content"""
        if entity_type == DataEntityType.TEXT_ITEM:
            if isinstance(content, dict):
                text_content = content.get('content', '')
                return f"Text item: {text_content[:100]}..." if len(text_content) > 100 else text_content
        elif entity_type == DataEntityType.DATASET:
            if isinstance(content, list):
                return f"Dataset with {len(content)} items"
            elif isinstance(content, dict):
                return f"Dataset: {content.get('name', 'Unnamed')}"
        elif entity_type == DataEntityType.LABEL:
            return f"Label: {content if isinstance(content, str) else str(content)}"
        
        return f"{entity_type.value}: {str(content)[:100]}"
    
    def _store_version(self, version: DataVersion, content: Any):
        """Store version and content in database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Store version metadata
        cursor.execute("""
            INSERT INTO data_versions 
            (id, entity_id, entity_type, version_number, change_type, created_at,
             created_by, data_hash, content_summary, metadata, parent_version_id, change_description)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (version.id, version.entity_id, version.entity_type.value, version.version_number,
              version.change_type.value, version.created_at, version.created_by, version.data_hash,
              version.content_summary, json.dumps(version.metadata), version.parent_version_id,
              version.change_description))
        
        # Store content snapshot
        content_str = json.dumps(content) if not isinstance(content, str) else content
        content_type = "json" if not isinstance(content, str) else "text"
        
        cursor.execute("""
            INSERT INTO data_snapshots (version_id, content, content_type)
            VALUES (?, ?, ?)
        """, (version.id, content_str, content_type))
        
        conn.commit()
        conn.close()
    
    def _create_lineage_node(self, version: DataVersion):
        """Create a lineage node for a version"""
        node = LineageNode(
            id=str(uuid.uuid4()),
            entity_id=version.entity_id,
            entity_type=version.entity_type,
            version_id=version.id,
            created_at=version.created_at,
            metadata={
                "version_number": version.version_number,
                "change_type": version.change_type.value,
                "created_by": version.created_by
            }
        )
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO lineage_nodes 
            (id, entity_id, entity_type, version_id, created_at, metadata)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (node.id, node.entity_id, node.entity_type.value, node.version_id,
              node.created_at, json.dumps(node.metadata)))
        
        conn.commit()
        conn.close()
    
    def _store_lineage_edge(self, edge: LineageEdge):
        """Store a lineage edge in database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO lineage_edges 
            (id, source_node_id, target_node_id, relationship_type, created_at, metadata)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (edge.id, edge.source_node_id, edge.target_node_id, edge.relationship_type,
              edge.created_at, json.dumps(edge.metadata)))
        
        conn.commit()
        conn.close()
    
    def _get_latest_lineage_node(self, entity_id: str) -> Optional[LineageNode]:
        """Get the latest lineage node for an entity"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT ln.* FROM lineage_nodes ln
            JOIN data_versions dv ON ln.version_id = dv.id
            WHERE ln.entity_id = ?
            ORDER BY dv.version_number DESC
            LIMIT 1
        """, (entity_id,))
        
        row = cursor.fetchone()
        conn.close()
        
        return self._row_to_lineage_node(row) if row else None
    
    def _store_in_git(self, version: DataVersion, content: Any):
        """Store version in Git repository"""
        if not self.repo:
            return
        
        try:
            # Create file path based on entity
            file_path = self.git_repo_path / f"{version.entity_type.value}s" / f"{version.entity_id}.json"
            file_path.parent.mkdir(exist_ok=True)
            
            # Write content to file
            with open(file_path, 'w') as f:
                json.dump({
                    "version_id": version.id,
                    "version_number": version.version_number,
                    "content": content,
                    "metadata": version.metadata
                }, f, indent=2)
            
            # Commit to Git
            self.repo.index.add([str(file_path)])
            commit_message = f"{version.change_type.value}: {version.entity_type.value} {version.entity_id} v{version.version_number}"
            if version.change_description:
                commit_message += f" - {version.change_description}"
            
            self.repo.index.commit(commit_message)
            
        except Exception as e:
            # Git operations are optional, don't fail the main operation
            pass
    
    def _log_audit_entry(self, entity_id: str, entity_type: DataEntityType, 
                        action: str, actor: str, details: Dict[str, Any],
                        ip_address: Optional[str] = None, user_agent: Optional[str] = None):
        """Log an audit entry"""
        entry = AuditEntry(
            id=str(uuid.uuid4()),
            entity_id=entity_id,
            entity_type=entity_type,
            action=action,
            actor=actor,
            timestamp=datetime.now().isoformat(),
            details=details,
            ip_address=ip_address,
            user_agent=user_agent
        )
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO audit_log 
            (id, entity_id, entity_type, action, actor, timestamp, details, ip_address, user_agent)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (entry.id, entry.entity_id, entry.entity_type.value, entry.action,
              entry.actor, entry.timestamp, json.dumps(entry.details),
              entry.ip_address, entry.user_agent))
        
        conn.commit()
        conn.close()
    
    def _compare_dicts(self, dict1: Dict, dict2: Dict, path: str = "") -> List[Dict[str, Any]]:
        """Compare two dictionaries and return differences"""
        changes = []
        
        # Check for added/modified keys
        for key in dict2:
            current_path = f"{path}.{key}" if path else key
            if key not in dict1:
                changes.append({
                    "type": "addition",
                    "path": current_path,
                    "new_value": dict2[key]
                })
            elif dict1[key] != dict2[key]:
                if isinstance(dict1[key], dict) and isinstance(dict2[key], dict):
                    changes.extend(self._compare_dicts(dict1[key], dict2[key], current_path))
                else:
                    changes.append({
                        "type": "modification",
                        "path": current_path,
                        "old_value": dict1[key],
                        "new_value": dict2[key]
                    })
        
        # Check for removed keys
        for key in dict1:
            if key not in dict2:
                current_path = f"{path}.{key}" if path else key
                changes.append({
                    "type": "deletion",
                    "path": current_path,
                    "old_value": dict1[key]
                })
        
        return changes
    
    def _compare_lists(self, list1: List, list2: List) -> List[Dict[str, Any]]:
        """Compare two lists and return differences"""
        changes = []
        
        if len(list1) != len(list2):
            changes.append({
                "type": "size_change",
                "old_size": len(list1),
                "new_size": len(list2)
            })
        
        # Simple comparison - could be more sophisticated
        for i, (item1, item2) in enumerate(zip(list1, list2)):
            if item1 != item2:
                changes.append({
                    "type": "item_change",
                    "index": i,
                    "old_value": item1,
                    "new_value": item2
                })
        
        return changes
    
    def _row_to_version(self, row) -> DataVersion:
        """Convert database row to DataVersion"""
        return DataVersion(
            id=row[0], entity_id=row[1], entity_type=DataEntityType(row[2]),
            version_number=row[3], change_type=ChangeType(row[4]), created_at=row[5],
            created_by=row[6], data_hash=row[7], content_summary=row[8],
            metadata=json.loads(row[9] or "{}"), parent_version_id=row[10],
            change_description=row[11] or ""
        )
    
    def _row_to_lineage_node(self, row) -> LineageNode:
        """Convert database row to LineageNode"""
        return LineageNode(
            id=row[0], entity_id=row[1], entity_type=DataEntityType(row[2]),
            version_id=row[3], created_at=row[4], metadata=json.loads(row[5] or "{}")
        )
    
    def _row_to_lineage_edge(self, row) -> LineageEdge:
        """Convert database row to LineageEdge"""
        return LineageEdge(
            id=row[0], source_node_id=row[1], target_node_id=row[2],
            relationship_type=row[3], created_at=row[4], metadata=json.loads(row[5] or "{}")
        )
    
    def _row_to_audit_entry(self, row) -> AuditEntry:
        """Convert database row to AuditEntry"""
        return AuditEntry(
            id=row[0], entity_id=row[1], entity_type=DataEntityType(row[2]),
            action=row[3], actor=row[4], timestamp=row[5], details=json.loads(row[6]),
            ip_address=row[7], user_agent=row[8]
        )
    
    def _lineage_node_to_dict(self, node: LineageNode) -> Dict[str, Any]:
        """Convert LineageNode to dictionary"""
        return {
            "id": node.id,
            "entity_id": node.entity_id,
            "entity_type": node.entity_type.value,
            "version_id": node.version_id,
            "created_at": node.created_at,
            "metadata": node.metadata
        }
    
    def _lineage_edge_to_dict(self, edge: LineageEdge) -> Dict[str, Any]:
        """Convert LineageEdge to dictionary"""
        return {
            "id": edge.id,
            "source_node_id": edge.source_node_id,
            "target_node_id": edge.target_node_id,
            "relationship_type": edge.relationship_type,
            "created_at": edge.created_at,
            "metadata": edge.metadata
        }

# Global data versioning system instance
data_versioning_system = DataVersioningSystem()
