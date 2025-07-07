"""
Data Versioning API Router
Provides endpoints for data versioning, lineage tracking, and audit trails
"""
from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List, Dict, Any
import sys
import os

# Add the parent directory to the path to import common modules
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from shared.storage.versioning.data_versioning import DataVersioningSystem

router = APIRouter(tags=["data-versioning"])

# Initialize data versioning system
versioning_system = DataVersioningSystem()

@router.get("/entities")
async def list_entities():
    """Get all versioned entities"""
    try:
        entities = versioning_system.list_entities()
        return {"status": "success", "entities": entities}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list entities: {str(e)}")

@router.post("/entities")
async def create_entity_version(version_data: Dict[str, Any]):
    """Create a new version of a data entity"""
    try:
        # Validate required fields
        required_fields = ['entity_id', 'data', 'change_type']
        for field in required_fields:
            if field not in version_data:
                raise HTTPException(status_code=400, detail=f"Missing required field: {field}")
        
        from common.data_versioning import DataEntityType, ChangeType
        
        result = versioning_system.create_version(
            entity_id=version_data['entity_id'],
            entity_type=DataEntityType.TEXT_ITEM,  # Default type, could be made configurable
            content=version_data['data'],
            created_by=version_data.get('user_id', 'system'),
            change_type=ChangeType(version_data['change_type']),
            change_description=version_data.get('description', ''),
            parent_version_id=version_data.get('parent_version')
        )
        
        return {"status": "success", "version_id": result}
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create entity version: {str(e)}")

@router.get("/entities/{entity_id}/versions")
async def get_entity_versions(
    entity_id: str,
    limit: int = Query(default=10, description="Maximum number of versions to return"),
    offset: int = Query(default=0, description="Number of versions to skip")
):
    """Get version history for an entity"""
    try:
        versions = versioning_system.get_entity_versions(entity_id, limit, offset)
        return {"status": "success", "versions": versions}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get entity versions: {str(e)}")

@router.get("/entities/{entity_id}/versions/{version_id}")
async def get_specific_version(entity_id: str, version_id: str):
    """Get a specific version of an entity"""
    try:
        version = versioning_system.get_version(entity_id, version_id)
        if not version:
            raise HTTPException(status_code=404, detail="Version not found")
        
        return {"status": "success", "version": version}
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get version: {str(e)}")

@router.get("/entities/{entity_id}/lineage")
async def get_entity_lineage(entity_id: str):
    """Get lineage information for an entity"""
    try:
        lineage = versioning_system.get_lineage(entity_id)
        return {"status": "success", "lineage": lineage}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get lineage: {str(e)}")

@router.post("/entities/{entity_id}/lineage")
async def add_lineage_relationship(entity_id: str, lineage_data: Dict[str, Any]):
    """Add a lineage relationship"""
    try:
        # Validate required fields
        required_fields = ['source_entity', 'relationship_type']
        for field in required_fields:
            if field not in lineage_data:
                raise HTTPException(status_code=400, detail=f"Missing required field: {field}")
        
        result = versioning_system.add_lineage(
            target_entity=entity_id,
            source_entity=lineage_data['source_entity'],
            relationship_type=lineage_data['relationship_type'],
            metadata=lineage_data.get('metadata', {})
        )
        
        return result
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to add lineage: {str(e)}")

@router.get("/audit-log")
async def get_audit_log(
    entity_id: Optional[str] = Query(default=None, description="Filter by entity ID"),
    user_id: Optional[str] = Query(default=None, description="Filter by user ID"),
    change_type: Optional[str] = Query(default=None, description="Filter by change type"),
    days: int = Query(default=7, description="Number of days to look back"),
    limit: int = Query(default=50, description="Maximum number of entries to return")
):
    """Get audit log entries"""
    try:
        filters = {}
        if entity_id:
            filters['entity_id'] = entity_id
        if user_id:
            filters['user_id'] = user_id
        if change_type:
            filters['change_type'] = change_type
        
        audit_log = versioning_system.get_audit_log(filters, days, limit)
        return {"status": "success", "audit_log": audit_log}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get audit log: {str(e)}")

@router.post("/entities/{entity_id}/compare")
async def compare_versions(entity_id: str, compare_request: Dict[str, Any]):
    """Compare two versions of an entity"""
    try:
        # Validate required fields
        required_fields = ['version1', 'version2']
        for field in required_fields:
            if field not in compare_request:
                raise HTTPException(status_code=400, detail=f"Missing required field: {field}")
        
        comparison = versioning_system.compare_entity_versions(
            entity_id=entity_id,
            version1=compare_request['version1'],
            version2=compare_request['version2']
        )
        
        return {"status": "success", "comparison": comparison}
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to compare versions: {str(e)}")

@router.post("/entities/{entity_id}/rollback")
async def rollback_entity(entity_id: str, rollback_request: Dict[str, Any]):
    """Rollback an entity to a previous version"""
    try:
        # Validate required fields
        if 'target_version' not in rollback_request:
            raise HTTPException(status_code=400, detail="Missing required field: target_version")
        
        result = versioning_system.rollback_entity_to_version(
            entity_id=entity_id,
            target_version=rollback_request['target_version'],
            user_id=rollback_request.get('user_id', 'system'),
            reason=rollback_request.get('reason', 'Manual rollback')
        )
        
        return result
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to rollback entity: {str(e)}")

@router.get("/analytics")
async def get_versioning_analytics(
    days: int = Query(default=7, description="Number of days for analytics")
):
    """Get versioning and lineage analytics"""
    try:
        analytics = versioning_system.get_analytics(days)
        return {"status": "success", "analytics": analytics}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get analytics: {str(e)}")

@router.get("/dashboard")
async def get_versioning_dashboard():
    """Get versioning dashboard data"""
    try:
        analytics = versioning_system.get_analytics(30)  # 30 days of data
        
        # Extract dashboard-specific metrics
        dashboard_data = {
            "overview": {
                "total_entities": analytics.get("total_entities", 0),
                "total_versions": analytics.get("total_versions", 0),
                "recent_changes": analytics.get("recent_changes", 0),
                "active_lineages": analytics.get("active_lineages", 0)
            },
            "change_types": analytics.get("change_type_distribution", {}),
            "recent_activity": analytics.get("recent_activity", [])[:10],
            "top_entities": analytics.get("most_versioned_entities", [])[:10],
            "trends": {
                "daily_changes": analytics.get("daily_changes", []),
                "user_activity": analytics.get("user_activity", {})
            }
        }
        
        return {"status": "success", "dashboard": dashboard_data}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get dashboard data: {str(e)}")

@router.get("/change-types")
async def get_change_types():
    """Get available change types"""
    return {
        "status": "success",
        "change_types": [
            {
                "type": "created",
                "description": "Entity was created"
            },
            {
                "type": "updated",
                "description": "Entity was updated"
            },
            {
                "type": "deleted",
                "description": "Entity was deleted"
            },
            {
                "type": "imported",
                "description": "Entity was imported from external source"
            },
            {
                "type": "merged",
                "description": "Entity was merged with another entity"
            },
            {
                "type": "split",
                "description": "Entity was split into multiple entities"
            },
            {
                "type": "labeled",
                "description": "Entity was labeled or re-labeled"
            },
            {
                "type": "validated",
                "description": "Entity was validated"
            },
            {
                "type": "corrected",
                "description": "Entity data was corrected"
            }
        ]
    }
