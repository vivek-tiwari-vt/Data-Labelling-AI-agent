"""
Integration Hub API Router
Provides endpoints for managing external data source integrations and synchronization
"""
from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List, Dict, Any
import sys
import os
from datetime import datetime
import uuid

# Add the parent directory to the path to import common modules
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from shared.integrations.integration_hub import IntegrationHub, ConnectionConfig, SyncJob

router = APIRouter(tags=["integration"])

# Initialize integration hub
integration_hub = IntegrationHub()

@router.get("/integrations")
async def list_integrations():
    """Get all integrations (alias for connections)"""
    try:
        connections = integration_hub.list_connections()
        return {"status": "success", "integrations": connections}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list integrations: {str(e)}")

@router.get("/services")
async def list_services():
    """Get all available service types"""
    try:
        services = integration_hub.list_service_types()
        return {"status": "success", "services": services}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list services: {str(e)}")

@router.get("/connections")
async def list_connections():
    """Get all configured connections"""
    try:
        connections = integration_hub.list_connections()
        return {"status": "success", "connections": connections}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list connections: {str(e)}")

@router.post("/connections")
async def create_connection(connection_data: Dict[str, Any]):
    """Create a new connection configuration"""
    try:
        # Validate required fields
        required_fields = ['name', 'type', 'endpoint', 'credentials']
        for field in required_fields:
            if field not in connection_data:
                raise HTTPException(status_code=400, detail=f"Missing required field: {field}")
        
        # Create connection config
        config = ConnectionConfig(
            name=connection_data['name'],
            type=connection_data['type'],
            endpoint=connection_data['endpoint'],
            credentials=connection_data['credentials'],
            metadata=connection_data.get('metadata', {}),
            created_at=datetime.now(),
            is_active=connection_data.get('is_active', True)
        )
        
        result = integration_hub.add_connection(config)
        
        if result['status'] == 'success':
            return result
        else:
            raise HTTPException(status_code=400, detail=result['message'])
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create connection: {str(e)}")

@router.post("/connections/{connection_name}/test")
async def test_connection(connection_name: str):
    """Test a connection to verify it's working"""
    try:
        result = integration_hub.test_connection(connection_name)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to test connection: {str(e)}")

@router.get("/connections/{connection_name}/resources")
async def list_connection_resources(connection_name: str, path: str = Query(default="", description="Resource path to list")):
    """List available resources for a connection"""
    try:
        connector = integration_hub.get_connector(connection_name)
        if not connector:
            raise HTTPException(status_code=404, detail="Connection not found")
        
        resources = connector.list_resources(path)
        return {"status": "success", "resources": resources}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list resources: {str(e)}")

@router.get("/connections/{connection_name}/schema")
async def get_resource_schema(connection_name: str, resource_path: str = Query(..., description="Resource path")):
    """Get schema information for a resource"""
    try:
        connector = integration_hub.get_connector(connection_name)
        if not connector:
            raise HTTPException(status_code=404, detail="Connection not found")
        
        schema = connector.get_schema(resource_path)
        return {"status": "success", "schema": schema}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get schema: {str(e)}")

@router.post("/connections/{connection_name}/fetch")
async def fetch_data(connection_name: str, fetch_request: Dict[str, Any]):
    """Fetch data from a connection resource"""
    try:
        connector = integration_hub.get_connector(connection_name)
        if not connector:
            raise HTTPException(status_code=404, detail="Connection not found")
        
        resource_path = fetch_request.get('resource_path')
        if not resource_path:
            raise HTTPException(status_code=400, detail="resource_path is required")
        
        filters = fetch_request.get('filters', {})
        data = connector.fetch_data(resource_path, filters)
        
        return {
            "status": "success",
            "data": data,
            "count": len(data)
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch data: {str(e)}")

@router.post("/sync-jobs")
async def create_sync_job(job_data: Dict[str, Any]):
    """Create a new data synchronization job"""
    try:
        # Validate required fields
        required_fields = ['connection_name', 'source_path', 'destination_path', 'sync_schedule']
        for field in required_fields:
            if field not in job_data:
                raise HTTPException(status_code=400, detail=f"Missing required field: {field}")
        
        # Create sync job
        sync_job = SyncJob(
            job_id=str(uuid.uuid4()),
            connection_name=job_data['connection_name'],
            source_path=job_data['source_path'],
            destination_path=job_data['destination_path'],
            sync_schedule=job_data['sync_schedule'],
            filters=job_data.get('filters', {})
        )
        
        result = integration_hub.create_sync_job(sync_job)
        return result
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create sync job: {str(e)}")

@router.post("/sync-jobs/{job_id}/execute")
async def execute_sync_job(job_id: str):
    """Execute a synchronization job"""
    try:
        result = integration_hub.sync_data(job_id)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to execute sync job: {str(e)}")

@router.get("/analytics")
async def get_integration_analytics(days: int = Query(default=7, description="Number of days for analytics")):
    """Get integration and synchronization analytics"""
    try:
        analytics = integration_hub.get_sync_analytics(days)
        return {"status": "success", "analytics": analytics}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get analytics: {str(e)}")

@router.get("/connector-types")
async def get_supported_connector_types():
    """Get list of supported connector types"""
    return {
        "status": "success",
        "connector_types": [
            {
                "type": "api",
                "name": "REST API",
                "description": "Connect to REST API endpoints",
                "required_credentials": ["headers"],
                "optional_credentials": ["auth_token", "api_key"]
            },
            {
                "type": "s3",
                "name": "Amazon S3",
                "description": "Connect to AWS S3 buckets",
                "required_credentials": ["access_key_id", "secret_access_key", "bucket"],
                "optional_credentials": ["region"]
            }
        ]
    }

@router.get("/connection-templates")
async def get_connection_templates():
    """Get predefined connection templates"""
    templates = [
        {
            "name": "GitHub API",
            "type": "api",
            "endpoint": "https://api.github.com",
            "description": "Connect to GitHub REST API",
            "credentials_template": {
                "headers": {
                    "Authorization": "token YOUR_GITHUB_TOKEN",
                    "Accept": "application/vnd.github.v3+json"
                }
            }
        },
        {
            "name": "JSONPlaceholder Demo",
            "type": "api",
            "endpoint": "https://jsonplaceholder.typicode.com",
            "description": "Demo API for testing",
            "credentials_template": {
                "headers": {
                    "Content-Type": "application/json"
                }
            }
        },
        {
            "name": "AWS S3 Bucket",
            "type": "s3",
            "endpoint": "s3.amazonaws.com",
            "description": "Connect to AWS S3 storage",
            "credentials_template": {
                "access_key_id": "YOUR_ACCESS_KEY",
                "secret_access_key": "YOUR_SECRET_KEY",
                "bucket": "your-bucket-name",
                "region": "us-west-2"
            }
        }
    ]
    
    return {"status": "success", "templates": templates}
