"""
Label Template Management API Router
Provides endpoints for managing label templates
"""
from fastapi import APIRouter, HTTPException, Query
from typing import Dict, Any, List, Optional
from pydantic import BaseModel
import sys
import os

# Add the parent directory to the path to import common modules
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from common.template_manager import template_manager

router = APIRouter()

class CreateTemplateRequest(BaseModel):
    name: str
    description: str
    labels: List[str]
    instructions: str
    domain: str = "general"
    is_public: bool = False

class UpdateTemplateRequest(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    labels: Optional[List[str]] = None
    instructions: Optional[str] = None
    domain: Optional[str] = None
    is_public: Optional[bool] = None

@router.get("/templates/")
async def list_templates(
    domain: Optional[str] = Query(None, description="Filter by domain"),
    search: Optional[str] = Query(None, description="Search templates"),
    popular: bool = Query(False, description="Get popular templates only")
):
    """List all available label templates with optional filtering."""
    try:
        if popular:
            templates = template_manager.get_popular_templates(limit=20)
        elif domain:
            templates = template_manager.get_templates_by_domain(domain)
        elif search:
            templates = template_manager.search_templates(search)
        else:
            templates = template_manager.get_all_templates()
        
        return {
            "templates": templates,
            "total": len(templates)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list templates: {str(e)}")

@router.get("/templates/{template_id}")
async def get_template(template_id: str):
    """Get a specific template by ID."""
    try:
        template = template_manager.get_template_by_id(template_id)
        if not template:
            raise HTTPException(status_code=404, detail="Template not found")
        
        return template
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get template: {str(e)}")

@router.post("/templates/")
async def create_template(template_request: CreateTemplateRequest):
    """Create a new label template."""
    try:
        # Validate labels
        if not template_request.labels:
            raise HTTPException(status_code=400, detail="At least one label is required")
        
        if len(template_request.labels) > 50:
            raise HTTPException(status_code=400, detail="Too many labels (max 50)")
        
        # Create template
        template_data = template_request.dict()
        template_id = template_manager.create_template(template_data)
        
        return {
            "template_id": template_id,
            "message": "Template created successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create template: {str(e)}")

@router.put("/templates/{template_id}")
async def update_template(template_id: str, update_request: UpdateTemplateRequest):
    """Update an existing template."""
    try:
        # Check if template exists
        existing_template = template_manager.get_template_by_id(template_id)
        if not existing_template:
            raise HTTPException(status_code=404, detail="Template not found")
        
        # Don't allow updating system templates
        if existing_template.get('created_by') == 'system':
            raise HTTPException(status_code=403, detail="Cannot modify system templates")
        
        # Update template
        updates = {k: v for k, v in update_request.dict().items() if v is not None}
        success = template_manager.update_template(template_id, updates)
        
        if not success:
            raise HTTPException(status_code=500, detail="Failed to update template")
        
        return {
            "message": "Template updated successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update template: {str(e)}")

@router.delete("/templates/{template_id}")
async def delete_template(template_id: str):
    """Delete a template."""
    try:
        # Check if template exists
        template = template_manager.get_template_by_id(template_id)
        if not template:
            raise HTTPException(status_code=404, detail="Template not found")
        
        success = template_manager.delete_template(template_id)
        
        if not success:
            raise HTTPException(status_code=403, detail="Cannot delete system templates")
        
        return {
            "message": "Template deleted successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete template: {str(e)}")

@router.post("/templates/{template_id}/use")
async def use_template(template_id: str):
    """Mark a template as used (increment usage counter)."""
    try:
        template = template_manager.get_template_by_id(template_id)
        if not template:
            raise HTTPException(status_code=404, detail="Template not found")
        
        template_manager.increment_usage(template_id)
        
        return {
            "message": "Template usage recorded",
            "template": template_manager.get_template_by_id(template_id)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to record template usage: {str(e)}")

@router.get("/templates/domains/list")
async def list_domains():
    """Get all available template domains."""
    try:
        templates = template_manager.get_all_templates()
        domains = list(set(t.get('domain', 'general') for t in templates))
        domains.sort()
        
        # Count templates per domain
        domain_counts = {}
        for template in templates:
            domain = template.get('domain', 'general')
            domain_counts[domain] = domain_counts.get(domain, 0) + 1
        
        return {
            "domains": domains,
            "domain_counts": domain_counts
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list domains: {str(e)}")

@router.get("/templates/analytics/")
async def get_template_analytics():
    """Get analytics about template usage and distribution."""
    try:
        analytics = template_manager.get_template_analytics()
        return analytics
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get analytics: {str(e)}")

@router.post("/templates/import")
async def import_template_from_job(job_id: str):
    """Create a template from a completed job's configuration."""
    try:
        from common.job_logger import job_logger
        
        # Get job log
        job_log = job_logger.get_job_log(job_id)
        if not job_log:
            raise HTTPException(status_code=404, detail="Job log not found")
        
        # Extract template data from job
        user_input = job_log.get("user_input", {})
        job_metadata = job_log.get("job_metadata", {})
        
        labels = user_input.get("available_labels", [])
        instructions = user_input.get("instructions", "")
        
        if not labels:
            raise HTTPException(status_code=400, detail="Job has no labels to import")
        
        # Create template name from job
        template_name = f"Job Template {job_id[:8]}"
        
        template_data = {
            "name": template_name,
            "description": f"Template created from job {job_id}",
            "labels": labels,
            "instructions": instructions,
            "domain": "imported",
            "is_public": False,
            "created_by": "import"
        }
        
        template_id = template_manager.create_template(template_data)
        
        return {
            "template_id": template_id,
            "message": f"Template created from job {job_id}",
            "template": template_manager.get_template_by_id(template_id)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to import template: {str(e)}")
