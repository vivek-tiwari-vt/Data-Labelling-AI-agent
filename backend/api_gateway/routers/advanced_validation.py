"""
Advanced Validation API Router
Provides endpoints for advanced validation rules and data quality checks
"""
from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List, Dict, Any
import sys
import os

# Add the parent directory to the path to import common modules
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from core.quality.advanced_validation import AdvancedValidationSystem

router = APIRouter(tags=["advanced-validation"])

# Initialize validation system
validation_engine = AdvancedValidationSystem()

@router.post("/rules")
async def create_validation_rule(rule_data: Dict[str, Any]):
    """Create a new validation rule"""
    try:
        # Validate required fields
        required_fields = ['name', 'description', 'rule_type', 'conditions']
        for field in required_fields:
            if field not in rule_data:
                raise HTTPException(status_code=400, detail=f"Missing required field: {field}")
        
        result = validation_engine.create_rule(
            name=rule_data['name'],
            description=rule_data['description'],
            rule_type=rule_data['rule_type'],
            conditions=rule_data['conditions'],
            severity=rule_data.get('severity', 'medium'),
            is_active=rule_data.get('is_active', True),
            metadata=rule_data.get('metadata', {})
        )
        
        return result
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create validation rule: {str(e)}")

@router.get("/rules")
async def list_validation_rules(
    rule_type: Optional[str] = Query(default=None, description="Filter by rule type"),
    severity: Optional[str] = Query(default=None, description="Filter by severity"),
    is_active: Optional[bool] = Query(default=None, description="Filter by active status")
):
    """List validation rules with optional filters"""
    try:
        filters = {}
        if rule_type:
            filters['rule_type'] = rule_type
        if severity:
            filters['severity'] = severity
        if is_active is not None:
            filters['is_active'] = is_active
        
        rules = validation_engine.get_rules(filters)
        return {"status": "success", "rules": rules}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list validation rules: {str(e)}")

@router.put("/rules/{rule_id}")
async def update_validation_rule(rule_id: str, updates: Dict[str, Any]):
    """Update an existing validation rule"""
    try:
        result = validation_engine.update_rule(rule_id, updates)
        
        if result['status'] == 'success':
            return result
        else:
            raise HTTPException(status_code=404, detail=result['message'])
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update validation rule: {str(e)}")

@router.delete("/rules/{rule_id}")
async def delete_validation_rule(rule_id: str):
    """Delete a validation rule"""
    try:
        result = validation_engine.delete_rule(rule_id)
        
        if result['status'] == 'success':
            return result
        else:
            raise HTTPException(status_code=404, detail=result['message'])
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete validation rule: {str(e)}")

@router.post("/validate")
async def validate_data(validation_request: Dict[str, Any]):
    """Validate data against rules"""
    try:
        # Validate required fields
        required_fields = ['data']
        for field in required_fields:
            if field not in validation_request:
                raise HTTPException(status_code=400, detail=f"Missing required field: {field}")
        
        data = validation_request['data']
        rule_types = validation_request.get('rule_types', None)
        job_id = validation_request.get('job_id', None)
        
        results = validation_engine.validate_data(data, rule_types, job_id)
        
        return {"status": "success", "validation_results": results}
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to validate data: {str(e)}")

@router.post("/validate-batch")
async def validate_batch_data(batch_request: Dict[str, Any]):
    """Validate multiple data items in batch"""
    try:
        # Validate required fields
        if 'data_items' not in batch_request:
            raise HTTPException(status_code=400, detail="Missing required field: data_items")
        
        data_items = batch_request['data_items']
        rule_types = batch_request.get('rule_types', None)
        job_id = batch_request.get('job_id', None)
        
        results = []
        for i, data_item in enumerate(data_items):
            try:
                validation_result = validation_engine.validate_data(data_item, rule_types, job_id)
                results.append({
                    "index": i,
                    "status": "success",
                    "validation_results": validation_result
                })
            except Exception as e:
                results.append({
                    "index": i,
                    "status": "error",
                    "message": str(e)
                })
        
        return {"status": "success", "batch_results": results}
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to validate batch data: {str(e)}")

@router.get("/results")
async def get_validation_results(
    job_id: Optional[str] = Query(default=None, description="Filter by job ID"),
    severity: Optional[str] = Query(default=None, description="Filter by severity"),
    status: Optional[str] = Query(default=None, description="Filter by status"),
    days: int = Query(default=7, description="Number of days to look back"),
    limit: int = Query(default=50, description="Maximum number of results to return")
):
    """Get validation results with optional filters"""
    try:
        filters = {}
        if job_id:
            filters['job_id'] = job_id
        if severity:
            filters['severity'] = severity
        if status:
            filters['status'] = status
        
        results = validation_engine.get_validation_results(filters, days, limit)
        return {"status": "success", "results": results}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get validation results: {str(e)}")

@router.get("/analytics")
async def get_validation_analytics(
    days: int = Query(default=7, description="Number of days for analytics")
):
    """Get validation analytics and quality metrics"""
    try:
        analytics = validation_engine.get_analytics(days)
        return {"status": "success", "analytics": analytics}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get analytics: {str(e)}")

@router.get("/rule-types")
async def get_rule_types():
    """Get available validation rule types"""
    return {
        "status": "success",
        "rule_types": [
            {
                "type": "format",
                "description": "Validate data format (email, phone, URL, etc.)",
                "example_conditions": {
                    "field": "email",
                    "pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
                }
            },
            {
                "type": "range",
                "description": "Validate numeric ranges",
                "example_conditions": {
                    "field": "age",
                    "min_value": 0,
                    "max_value": 120
                }
            },
            {
                "type": "length",
                "description": "Validate string length",
                "example_conditions": {
                    "field": "description",
                    "min_length": 10,
                    "max_length": 500
                }
            },
            {
                "type": "required",
                "description": "Validate required fields",
                "example_conditions": {
                    "required_fields": ["name", "email", "label"]
                }
            },
            {
                "type": "consistency",
                "description": "Validate data consistency rules",
                "example_conditions": {
                    "field1": "start_date",
                    "field2": "end_date",
                    "operator": "less_than"
                }
            },
            {
                "type": "business",
                "description": "Custom business logic validation",
                "example_conditions": {
                    "logic": "if label == 'positive' then sentiment_score > 0.5"
                }
            },
            {
                "type": "duplicate",
                "description": "Check for duplicate records",
                "example_conditions": {
                    "unique_fields": ["id", "email"]
                }
            },
            {
                "type": "quality",
                "description": "Data quality checks",
                "example_conditions": {
                    "field": "text",
                    "checks": ["no_empty_strings", "no_special_chars", "min_words"]
                }
            }
        ]
    }

@router.get("/templates")
async def get_validation_templates():
    """Get predefined validation rule templates"""
    templates = [
        {
            "name": "Email Format Validation",
            "description": "Validate email addresses",
            "template": {
                "name": "Email Format Check",
                "description": "Ensures email fields contain valid email addresses",
                "rule_type": "format",
                "conditions": {
                    "field": "email",
                    "pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
                },
                "severity": "high"
            }
        },
        {
            "name": "Required Fields Check",
            "description": "Ensure all required fields are present",
            "template": {
                "name": "Required Fields Validation",
                "description": "Checks that all mandatory fields are present and not empty",
                "rule_type": "required",
                "conditions": {
                    "required_fields": ["id", "text", "label"]
                },
                "severity": "high"
            }
        },
        {
            "name": "Text Quality Check",
            "description": "Validate text field quality",
            "template": {
                "name": "Text Quality Validation",
                "description": "Ensures text fields meet quality standards",
                "rule_type": "quality",
                "conditions": {
                    "field": "text",
                    "min_length": 5,
                    "max_length": 1000,
                    "min_words": 2,
                    "no_special_chars": True
                },
                "severity": "medium"
            }
        },
        {
            "name": "Label Consistency",
            "description": "Validate label consistency with data",
            "template": {
                "name": "Label Consistency Check",
                "description": "Ensures labels are consistent with text content",
                "rule_type": "business",
                "conditions": {
                    "logic": "if contains_positive_words(text) and label == 'negative' then flag_inconsistency"
                },
                "severity": "medium"
            }
        },
        {
            "name": "Duplicate Detection",
            "description": "Detect duplicate records",
            "template": {
                "name": "Duplicate Record Check",
                "description": "Identifies potential duplicate records",
                "rule_type": "duplicate",
                "conditions": {
                    "unique_fields": ["text"],
                    "similarity_threshold": 0.9
                },
                "severity": "low"
            }
        }
    ]
    
    return {"status": "success", "templates": templates}

@router.get("/dashboard")
async def get_validation_dashboard():
    """Get validation dashboard data"""
    try:
        analytics = validation_engine.get_analytics(30)  # 30 days of data
        
        # Extract dashboard-specific metrics
        dashboard_data = {
            "overview": {
                "total_validations": analytics.get("total_validations", 0),
                "failed_validations": analytics.get("failed_validations", 0),
                "success_rate": analytics.get("success_rate", 0),
                "active_rules": analytics.get("active_rules", 0)
            },
            "severity_distribution": analytics.get("severity_distribution", {}),
            "rule_type_usage": analytics.get("rule_type_usage", {}),
            "recent_failures": analytics.get("recent_failures", [])[:10],
            "trends": {
                "daily_validations": analytics.get("daily_validations", []),
                "quality_trends": analytics.get("quality_trends", [])
            }
        }
        
        return {"status": "success", "dashboard": dashboard_data}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get dashboard data: {str(e)}")

@router.post("/validate/{job_id}")
async def validate_job(job_id: str):
    """Validate a completed job's results against validation rules"""
    try:
        # Import job logger to get job data
        import sys
        import os
        sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
        from infrastructure.monitoring.job_logger import job_logger
        
        # Get job data
        job_log = job_logger.get_job_log(job_id)
        if not job_log:
            raise HTTPException(status_code=404, detail="Job not found")
        
        if job_log.get("status") != "completed":
            raise HTTPException(status_code=400, detail="Job must be completed to validate")
        
        # Extract job results for validation
        text_agent_data = job_log.get("text_agent", {})
        processing_details = text_agent_data.get("processing_details", [])
        
        # Prepare data for validation
        validation_data = []
        for detail in processing_details:
            item = {
                "text_id": detail.get("text_id", ""),
                "content": detail.get("content_preview", ""),
                "assigned_label": detail.get("assigned_label", ""),
                "confidence_score": detail.get("confidence_score", 0),
                "processing_time_ms": detail.get("processing_time_ms", 0)
            }
            validation_data.append(item)
        
        # Validate the job data
        results = validation_engine.validate_data(validation_data, job_id=job_id)
        
        return {
            "status": "success", 
            "job_id": job_id,
            "validation_results": results,
            "total_items": len(validation_data)
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to validate job: {str(e)}")
